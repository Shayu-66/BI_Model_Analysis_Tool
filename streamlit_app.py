# 导入必要的库
import json
import pandas as pd
import re
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import zipfile
import io
# openpyxl is only required when exporting to Excel. Delay import to the export
# function to avoid ModuleNotFoundError on app startup when the package is
# missing in the runtime. If missing, we show a friendly message to the user.
_OPENPYXL_AVAILABLE = True
try:
    import openpyxl  # quick availability check
except ModuleNotFoundError:
    _OPENPYXL_AVAILABLE = False

# 在模块级别导入Streamlit，但不在模块级别使用任何Streamlit函数
# 这是Streamlit的推荐做法，可以避免某些导入相关的问题
import streamlit as st
import time

# 改进的防抖函数 - 简化实现并确保实时响应
# 使用更直接的方法，确保每次输入变化都能正确触发搜索更新
# key_prefix: 用于标识不同搜索框的前缀
def debounced_search(key_prefix):
    # 生成唯一的session_state键名
    search_key = f"{key_prefix}_search_term"
    
    # 初始化session_state中的变量
    if search_key not in st.session_state:
        st.session_state[search_key] = ""
    
    return st.session_state[search_key]

# 简化的搜索状态更新函数
def update_search_timer(key_prefix, input_value):
    # 直接更新搜索词，去掉防抖延迟，确保实时响应
    search_key = f"{key_prefix}_search_term"
    st.session_state[search_key] = input_value  # 直接设置搜索词，实现即时搜索

class BIMParser:
    """BIM文件解析器"""
    
    def __init__(self):
        self.raw_data = None
        self.tables_info = []
        self.columns_info = []
        self.measures_info = []
        self.relationships_info = []
        self.overview_info = []
    
    def parse_file(self, file_content: str) -> Dict:
        """解析BIM文件或TMSL脚本内容"""
        try:
            # 重置解析结果
            self.raw_data = None
            self.tables_info = []
            self.columns_info = []
            self.measures_info = []
            self.relationships_info = []
            self.overview_info = []

            # 尝试将传入内容解析为 JSON（大部分 .bim / TMSL 为 JSON 格式）
            try:
                parsed = json.loads(file_content)
            except Exception as e_json:
                # 返回更友好的错误信息，便于调试上传/粘贴的问题
                return {"success": False, "error": f"无法解析为JSON: {str(e_json)}"}

            # 试图定位模型对象：多数 .bim / TMSL JSON 包含一个名为 "model" 的子对象
            def _locate_model(obj):
                # 直接包含 model 键
                if isinstance(obj, dict):
                    if 'model' in obj and isinstance(obj['model'], dict):
                        return obj['model']
                    # 常见命名：SemanticModel
                    if 'SemanticModel' in obj and isinstance(obj['SemanticModel'], dict):
                        return obj['SemanticModel']
                    # 如果当前对象看起来就是模型（包含 tables 键）
                    if 'tables' in obj and isinstance(obj['tables'], list):
                        return obj
                    # 递归查找子对象
                    for v in obj.values():
                        if isinstance(v, (dict, list)):
                            found = _locate_model(v)
                            if found is not None:
                                return found
                elif isinstance(obj, list):
                    for item in obj:
                        if isinstance(item, (dict, list)):
                            found = _locate_model(item)
                            if found is not None:
                                return found
                return None

            model_obj = _locate_model(parsed)
            if model_obj is None:
                # 如果没有找到模型对象，保留原始解析结果以便错误追踪
                self.raw_data = parsed
            else:
                # 统一把 raw_data 设置为包含 model 键的结构，方便后续解析函数使用
                self.raw_data = {'model': model_obj}

            # 填充解析信息
            self._parse_tables()
            self._parse_columns()
            self._parse_measures()
            self._parse_relationships()
            self._generate_overview()
            self._resolve_all_measure_references()

            # 打印调试信息（在控制台可见）
            print(f"解析结果 - 表数量: {len(self.tables_info)}")
            print(f"解析结果 - 列数量: {len(self.columns_info)}")
            print(f"解析结果 - 度量值数量: {len(self.measures_info)}")
            print(f"解析结果 - 关系数量: {len(self.relationships_info)}")

            return {
                "success": True,
                "tables": self.tables_info,
                "columns": self.columns_info,
                "measures": self.measures_info,
                "relationships": self.relationships_info,
                "overview": self.overview_info
            }
        except Exception as e:
            print(f"解析错误: {str(e)}")
            return {"success": False, "error": str(e)}
    
    def _parse_tables(self):
        """解析表信息"""
        if "model" not in self.raw_data or "tables" not in self.raw_data["model"]:
            print("警告: 模型数据中未找到表信息")
            return
        
        # 记录初始表数量
        print(f"开始解析表信息，原始表数量: {len(self.raw_data['model']['tables'])}")
            
        tables = self.raw_data["model"]["tables"]
        
        # 系统表列表，需要排除
        system_tables = ["User_用户权限表"]
        
        for table in tables:
            table_name = table.get("name", "")
            
            # 排除系统表
            if table_name in system_tables:
                print(f"排除系统表: {table_name}")
                continue
                
            source_table = "DAX创建"  # 默认值
            
            # 查找源表名逻辑
            if "partitions" in table and table["partitions"]:
                for partition in table["partitions"]:
                    if "source" in partition and "expression" in partition["source"]:
                        expression = partition["source"]["expression"]
                        source_table = self._extract_source_table(expression)
                        break
            
            # 计算分区数量
            partition_count = 0
            if "partitions" in table:
                partition_count = len(table["partitions"])
            
            self.tables_info.append({
                "表名": table_name,
                "源表名": source_table,
                "表分区数量": partition_count
            })
    
    def _extract_source_table(self, expression: List[str]) -> str:
        """从M函数表达式中提取源表名"""
        if not isinstance(expression, list):
            return "DAX创建"
        
        expression_text = "\n".join(expression)
        
        # 匹配 M 函数的 Item 方式
        item_pattern = r'Item="([^"]+)"'
        item_match = re.search(item_pattern, expression_text)
        if item_match:
            return item_match.group(1)
        
        # 匹配 SQL 的 FROM 语句
        from_pattern = r'FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)'
        from_match = re.search(from_pattern, expression_text, re.IGNORECASE)
        if from_match:
            return from_match.group(1)
        
        return "DAX创建"
    
    def _extract_connection_info(self, expression: List[str]) -> Tuple[str, str]:
        """从M函数表达式中提取实例地址和数据库名"""
        if not isinstance(expression, list):
            return "", ""
        
        expression_text = "\n".join(expression)
        
        # 模式1: 匹配 Value.NativeQuery 中的连接字符串格式
        # 例如: Value.NativeQuery(#"MySql/rm-2zeu9er24zw4831e6 mysql rds aliyuncs com:3306;data_mart",...)  
        native_query_pattern = r'Value\.NativeQuery\(#"([^;]+);([^"]+)"'
        native_query_match = re.search(native_query_pattern, expression_text)
        if native_query_match:
            instance_address = native_query_match.group(1)
            db_name = native_query_match.group(2)
            return instance_address, db_name
        
        # 模式2: 匹配 Source = #"" 格式
        # 例如: Source = #"MySql/rm-2zeu9er24zw4831e6 mysql rds aliyuncs com:3306;data_mart"
        source_pattern = r'Source\s*=\s*#"([^;]+);([^"]+)"'
        source_match = re.search(source_pattern, expression_text)
        if source_match:
            instance_address = source_match.group(1)
            db_name = source_match.group(2)
            return instance_address, db_name
        
        # 模式3: 从 Schema 字段中提取数据库名
        schema_pattern = r'Schema="([^"]+)"'
        schema_match = re.search(schema_pattern, expression_text)
        if schema_match:
            # 如果找到Schema但没有找到完整的连接信息
            # 尝试只提取数据库名
            db_name = schema_match.group(1)
            return "", db_name
        
        return "", ""
    
    def _parse_columns(self):
        """解析列信息"""
        if "model" not in self.raw_data or "tables" not in self.raw_data["model"]:
            return
            
        tables = self.raw_data["model"]["tables"]
        
        for table in tables:
            table_name = table.get("name", "")
            source_table = "DAX创建"
            
            # 获取源表名（复用解析逻辑）
            if "partitions" in table and table["partitions"]:
                for partition in table["partitions"]:
                    if "source" in partition and "expression" in partition["source"]:
                        expression = partition["source"]["expression"]
                        source_table = self._extract_source_table(expression)
                        break
            
            if "columns" in table:
                for column in table["columns"]:
                    column_name = column.get("name", "")
                    data_type = column.get("dataType", "")
                    
                    # 优先从M函数的Table.RenameColumns中查找源列名（新版本逻辑）
                    source_column = self._extract_column_source_from_m_function(table_name, column_name)
                    
                    # 如果M函数中找不到，使用sourceColumn字段
                    if source_column == column_name:
                        source_column = column.get("sourceColumn", "")
                    
                    # 如果sourceColumn也没有，保留列名本身
                    if not source_column:
                        source_column = column_name
                    
                    # 字段格式
                    format_string = column.get("formatString", "")
                    
                    self.columns_info.append({
                        "表名": table_name,
                        "源表名": source_table,
                        "列名": column_name,
                        "源列名": source_column,
                        "字段格式": data_type
                    })
    
    def _extract_column_source_from_m_function(self, table_name: str, column_name: str) -> str:
        """从M函数中提取列的源列名"""
        if "model" not in self.raw_data or "tables" not in self.raw_data["model"]:
            return column_name
        
        tables = self.raw_data["model"]["tables"]
        
        # 查找对应的表
        target_table = None
        for table in tables:
            if table.get("name", "") == table_name:
                target_table = table
                break
        
        if not target_table or "partitions" not in target_table:
            return column_name
        
        # 在所有分区中查找Table.RenameColumns映射
        for partition in target_table["partitions"]:
            if "source" in partition and "expression" in partition["source"]:
                expression = partition["source"]["expression"]
                
                # 查找Table.RenameColumns映射
                rename_mappings = self._extract_rename_mappings_from_m(expression)
                
                # 如果找到列名的映射，返回源列名
                for original_name, source_name in rename_mappings.items():
                    if original_name == column_name:
                        return source_name
        
        return column_name
    
    def _extract_rename_mappings_from_m(self, expression: str) -> dict:
        """从M表达式中提取Table.RenameColumns映射"""
        rename_mappings = {}
        
        # 处理expression可能是列表的情况
        if isinstance(expression, list):
            expression = " ".join(expression)
        
        # 查找Table.RenameColumns模式
        # 模式：Table.RenameColumns(#table(...), {"old1", "new1"}, {"old2", "new2"}, ...)
        import re
        
        # 查找Table.RenameColumns函数调用
        rename_pattern = r'Table\.RenameColumns\([^,]+,\s*(\{[^}]*(?:\{[^}]*}[^}]*)*})\)'
        matches = re.findall(rename_pattern, expression)
        
        for match in matches:
            # 提取映射对
            mapping_pairs = re.findall(r'\{\s*"([^"]+)"\s*,\s*"([^"]+)"\s*}', match)
            
            for old_name, new_name in mapping_pairs:
                rename_mappings[new_name] = old_name  # 映射是 new -> old
        
        # 也尝试匹配不带引号的映射
        if not rename_mappings:
            for match in matches:
                mapping_pairs = re.findall(r'\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*,\s*"([^"]+)"\s*}', match)
                for old_name, new_name in mapping_pairs:
                    rename_mappings[new_name] = old_name
        
        return rename_mappings
    
    def _parse_measures(self):
        """解析度量值信息"""
        if "model" not in self.raw_data or "tables" not in self.raw_data["model"]:
            return
            
        # 首先构建表名到源表名的lookup表和列名到源列名的lookup表
        table_source_lookup = {}
        column_source_lookup = {}
        tables = self.raw_data["model"]["tables"]
        
        # 构建表名到源表名的映射
        for table in tables:
            table_name = table.get("name", "")
            source_table = "DAX创建"
            
            # 查找源表名逻辑
            if "partitions" in table and table["partitions"]:
                for partition in table["partitions"]:
                    if "source" in partition and "expression" in partition["source"]:
                        expression = partition["source"]["expression"]
                        source_table = self._extract_source_table(expression)
                        break
            
            table_source_lookup[table_name] = source_table
            
            # 构建列名到源列名的映射
            if "columns" in table:
                for column in table["columns"]:
                    column_name = column.get("name", "")
                    source_column = column.get("sourceColumn", column_name)
                    
                    # 尝试从M函数中获取更准确的源列名
                    m_source_column = self._extract_column_source_from_m_function(table_name, column_name)
                    if m_source_column != column_name:
                        source_column = m_source_column
                    
                    column_source_lookup[f"{table_name}.{column_name}"] = source_column
        
        # 先收集所有度量值信息，用于后续解析引用
        all_measures = []
        for table in tables:
            if "measures" in table:
                for measure in table["measures"]:
                    measure_name = measure.get("name", "")
                    expression = measure.get("expression", "")
                    
                    # 处理数组格式的expression
                    if isinstance(expression, list):
                        expression = " ".join(expression)
                    
                    format_string = measure.get("formatString", "")
                    display_folder = measure.get("displayFolder", "")
                    table_name = table.get("name", "")
                    
                    # 替换转义字符
                    expression = expression.replace('\\"', '"')
                    
                    all_measures.append({
                        "度量值名称": measure_name,
                        "度量值计算逻辑": expression,
                        "度量值数据类型": format_string,
                        "度量值文件夹": display_folder,
                        "所属表": table_name
                    })
        
        # 创建度量值查找字典
        measure_lookup = {measure["度量值名称"]: measure for measure in all_measures}
        
        # 为每个度量值解析涉及的表、列和引用的度量值
        for measure in all_measures:
            measure_name = measure["度量值名称"]
            expression = measure["度量值计算逻辑"]
            
            # 提取当前表达式中的表和列
            current_tables = self._extract_involved_tables(expression)
            current_columns = self._extract_involved_columns(expression)
            
            # 递归查找引用的度量值的DAX逻辑，并合并表和列信息
            all_tables = set(current_tables)
            all_columns = set(current_columns)
            visited_measures = set()  # 避免循环引用
            
            def resolve_measure_references(measure_expr):
                # 查找引用的度量值
                measure_pattern = r"\[([^\]]+)\]"
                matches = re.findall(measure_pattern, measure_expr)
                
                for match in matches:
                    # 检查是否是已定义的度量值
                    if match in measure_lookup and match not in visited_measures:
                        visited_measures.add(match)
                        referenced_measure = measure_lookup[match]
                        # 合并被引用度量值涉及的表和列
                        ref_tables = self._extract_involved_tables(referenced_measure["度量值计算逻辑"])
                        ref_columns = self._extract_involved_columns(referenced_measure["度量值计算逻辑"])
                        all_tables.update(ref_tables)
                        all_columns.update(ref_columns)
                        # 递归处理嵌套引用
                        resolve_measure_references(referenced_measure["度量值计算逻辑"])
            
            # 开始递归解析引用
            resolve_measure_references(expression)
            
            # 格式化涉及表（使用与表关系页相同的显示方式）
            formatted_tables = []
            for table_involved in all_tables:
                source_table = table_source_lookup.get(table_involved, "DAX创建")
                formatted_tables.append(f"{table_involved} (源表: {source_table})")
            
            # 格式化涉及列（使用与表关系页相同的显示方式，从column_source_lookup获取源列名）
            formatted_columns = []
            for table_involved in all_tables:
                # 查找当前表中涉及的列
                table_related_columns = []
                for column_involved in all_columns:
                    # 尝试从DAX表达式中提取列所属的表
                    # 通过正则表达式匹配 '表名'[列名] 模式
                    column_pattern = rf"'{re.escape(table_involved)}'\[([^\]]+)\]"
                    column_matches = re.findall(column_pattern, expression)
                    
                    # 检查列是否属于当前表
                    if column_involved in column_matches:
                        table_related_columns.append(column_involved)
                
                # 为每个列格式化显示，只显示列名和源列
                for column_involved in table_related_columns:
                    source_column = column_source_lookup.get(f"{table_involved}.{column_involved}", column_involved)
                    formatted_columns.append(f"{column_involved} (源列: {source_column})")
            
            # 将解析结果添加到最终列表
            self.measures_info.append({
                "度量值名称": measure_name,
                "度量值计算逻辑": expression,
                "度量值数据类型": measure["度量值数据类型"],
                "度量值文件夹": measure["度量值文件夹"],
                "度量值涉及表": "\n".join(formatted_tables),
                "度量值涉及列": "\n".join(formatted_columns)
            })
    
    def _extract_involved_tables(self, expression: str) -> List[str]:
        """从DAX表达式中提取涉及的表"""
        tables = []
        # 匹配 '表名'[列名] 模式
        table_pattern = r"'([^']+)'"
        matches = re.findall(table_pattern, expression)
        tables.extend(matches)
        return list(set(tables))
    
    def _extract_involved_columns(self, expression: str) -> List[str]:
        """从DAX表达式中提取涉及的列"""
        columns = []
        # 匹配 '表名'[列名] 模式，提取列名
        column_pattern = r"'[^']+'" + r"\[" + r"'([^']+)'" + r"\]|'[^']+'" + r"\[([^\]]+)\]"
        matches = re.findall(column_pattern, expression)
        for match in matches:
            if isinstance(match, tuple):
                columns.extend([m for m in match if m])
            else:
                columns.append(match)
        return list(set(columns))
    
    def _parse_relationships(self):
        """解析表关系信息"""
        if "model" not in self.raw_data or "relationships" not in self.raw_data["model"]:
            return
            
        # 首先构建表名到源表名的lookup表
        table_source_lookup = {}
        tables = self.raw_data["model"]["tables"]
        
        for table in tables:
            table_name = table.get("name", "")
            source_table = "DAX创建"
            
            # 查找源表名逻辑（复用之前的逻辑）
            if "partitions" in table and table["partitions"]:
                for partition in table["partitions"]:
                    if "source" in partition and "expression" in partition["source"]:
                        expression = partition["source"]["expression"]
                        source_table = self._extract_source_table(expression)
                        break
            
            table_source_lookup[table_name] = source_table
        
        # 构建列名到源列名的lookup表
        column_source_lookup = {}
        for table in tables:
            table_name = table.get("name", "")
            
            if "columns" in table:
                for column in table["columns"]:
                    column_name = column.get("name", "")
                    source_column = column.get("sourceColumn", column_name)
                    
                    # 尝试从M函数中获取更准确的源列名
                    m_source_column = self._extract_column_source_from_m_function(table_name, column_name)
                    if m_source_column != column_name:
                        source_column = m_source_column
                    
                    column_source_lookup[f"{table_name}.{column_name}"] = source_column
        
        # 解析关系
        relationships = self.raw_data["model"]["relationships"]
        
        for relationship in relationships:
            from_table = relationship.get("fromTable", "")
            from_column = relationship.get("fromColumn", "")
            to_table = relationship.get("toTable", "")
            to_column = relationship.get("toColumn", "")
            # 解析关系类型
            to_cardinality = relationship.get("toCardinality", "")
            cardinality = "多对多" if to_cardinality else "一对多"
            
            # 解析筛选方向
            cross_filtering_behavior = relationship.get("crossFilteringBehavior", "")
            cross_filtering_behavior = "双向" if cross_filtering_behavior else "单向"
            
            # 解析是否活动
            is_active = relationship.get("isActive", True)
            security_filtering_behavior = "未启用" if is_active is False else "启用"
            
            # 获取源表名
            from_source_table = table_source_lookup.get(from_table, "DAX创建")
            to_source_table = table_source_lookup.get(to_table, "DAX创建")
            
            # 获取源列名
            from_source_column = column_source_lookup.get(f"{from_table}.{from_column}", from_column)
            to_source_column = column_source_lookup.get(f"{to_table}.{to_column}", to_column)
            
            self.relationships_info.append({
                "源表名": f"{from_table}\n(源表: {from_source_table})",
                "源表字段": f"{from_column}\n(源列: {from_source_column})",
                "目标表名": f"{to_table}\n(源表: {to_source_table})",
                "目标表字段": f"{to_column}\n(源列: {to_source_column})",
                "关系类型": cardinality,
                "筛选方向": cross_filtering_behavior,
                "是否活动": security_filtering_behavior
            })
    
    def _resolve_all_measure_references(self):
        """处理所有度量值之间的引用关系"""
        # 首先创建一个度量值名称到其信息的映射
        measure_lookup = {}
        for measure in self.measures_info:
            measure_lookup[measure["度量值名称"]] = measure
        
        # 更新每个度量值，添加对其他度量值的引用信息
        for measure in self.measures_info:
            expression = measure["度量值计算逻辑"]
            referenced_measures = []
            
            # 查找引用的度量值（假设度量值在表达式中以 [度量值名称] 格式出现）
            measure_pattern = r"\[([^\]]+)\]"
            matches = re.findall(measure_pattern, expression)
            
            for match in matches:
                # 排除可能的列引用（通过上下文判断）
                # 这里简化处理，假设没有表限定的就是度量值
                if match in measure_lookup and match not in referenced_measures:
                    referenced_measures.append(match)
            
            # 将引用的度量值信息添加到当前度量值中
            if referenced_measures:
                measure["度量值引用"] = "\n".join(referenced_measures)
            else:
                measure["度量值引用"] = ""
    
    def _generate_overview(self):
        """生成模型概览信息"""
        if "model" not in self.raw_data:
            return
        
        # 获取所有表的信息
        tables = self.raw_data["model"].get("tables", [])
        
        # 首先解析所有度量值，收集每个表涉及的度量值
        table_measure_counts = {}
        for table in tables:
            table_name = table.get("name", "")
            # 初始化每个表的度量值计数为0
            table_measure_counts[table_name] = 0
        
        # 计算每个表相关的度量值数量
        for table in tables:
            if "measures" in table:
                for measure in table["measures"]:
                    expression = measure.get("expression", "")
                    # 处理数组格式的expression
                    if isinstance(expression, list):
                        expression = " ".join(expression)
                    # 提取涉及的表
                    involved_tables = self._extract_involved_tables(expression)
                    # 如果度量值涉及此表，则将该表的度量值计数加1
                    for involved_table in involved_tables:
                        if involved_table in table_measure_counts:
                            table_measure_counts[involved_table] += 1
        
        # 生成概览信息
        for table in tables:
            table_name = table.get("name", "")
            
            # 统计列数和分区数
            column_count = len(table.get("columns", []))
            partition_count = len(table.get("partitions", []))
            
            # 获取源表名和连接信息
            source_table = "DAX创建"
            instance_address = ""
            database_name = ""
            protocol = ""
            
            if "partitions" in table and table["partitions"]:
                for partition in table["partitions"]:
                    if "source" in partition and "expression" in partition["source"]:
                        expression = partition["source"]["expression"]
                        source_table = self._extract_source_table(expression)
                        # 提取实例地址和数据库名
                        instance_address, database_name = self._extract_connection_info(expression)
                        # 提取协议类型
                        if instance_address and '/' in instance_address:
                            protocol = instance_address.split('/')[0]
                        break
            
            # 获取该表涉及的度量值数量
            measure_count = table_measure_counts.get(table_name, 0)
            
            self.overview_info.append({
                "表名": table_name,
                "源表名": source_table,
                "列数": column_count,
                "度量值数": measure_count,
                "分区数": partition_count,
                "实例地址": instance_address,
                "数据库名": database_name,
                "协议": protocol
            })

def create_streamlit_app():
    """创建Streamlit应用"""
    # 设置页面配置
    st.set_page_config(
        page_title="BI模型解析工具",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # 添加全局CSS样式
    st.markdown("""
    <style>
    /* 侧边栏样式优化 */
    [data-testid="stSidebar"] {
        background-color: #f8f9fa;
    }
    
    /* 文件上传区域样式优化 */
    [data-testid="stFileUploader"] {
        border: 2px dashed #0066cc;
        border-radius: 0.5rem;
        padding: 1rem;
        background-color: #f0f7ff;
    }
    
    /* 标题样式优化 */
    h1, h2, h3, h4 {
        color: #1a1a1a;
        font-weight: bold;
        font-family: 'Microsoft YaHei', Arial, sans-serif;
    }
    
    /* 按钮样式优化 */
    [data-baseweb="button"] {
        background-color: #0066cc !important;
        color: white !important;
        border-radius: 0.25rem !important;
        font-weight: bold !important;
    }
    
    /* 设置Streamlit根容器和主内容区域背景为黑色 */
    [data-testid="stApp"] {
        background-color: #000000 !important;
    }
    
    [data-testid="stAppViewContainer"] {
        background-color: #000000 !important;
    }
    
    /* 设置侧边栏背景为黑色 */
    [data-testid="stSidebar"] {
        background-color: #000000 !important;
    }
    
    /* 全局样式重置 - 确保所有页面所有表格元素靠左并设置白色字体 */
    body * {
        --st-text-align: left !important;
        color: #ffffff !important;
    }
    
    /* 确保内容区域可读性 */
    .main .block-container {
        background-color: #000000 !important;
        color: #ffffff !important;
    }
    
    /* 确保所有文本元素为白色 */
    h1, h2, h3, h4, h5, h6, p, span, div, label, button {
        color: #ffffff !important;
    }
    
    /* 确保表格元素为白色 */
    table, th, td {
        color: #ffffff !important;
        border-color: #333333 !important;
    }
    
    /* 确保输入框和选择框的可读性 */
    input, select, textarea {
        background-color: #333333 !important;
        color: #ffffff !important;
        border-color: #555555 !important;
    }
    
    /* 设置文件上传框体背景为黑色 */
    .st-emotion-cache-1sv6ehc {
        background-color: #000000 !important;
        border-color: #555555 !important;
    }
    
    /* 确保所有文件上传相关元素为黑色背景 */
    .stFileUploader, .st-file-uploader {
        background-color: #000000 !important;
    }
    
    /* 确保文件上传按钮的样式 */
    .st-emotion-cache-166asn9 {
        background-color: #000000 !important;
        border-color: #555555 !important;
    }
    
    /* 确保拖放区域的样式 */
    .st-dg {
        background-color: #000000 !important;
        border-color: #555555 !important;
    }
    
    /* 确保侧边栏折叠和展开按键始终显示 - 包括所有状态 */
    /* 折叠按钮 (向右箭头) */
    [data-testid="stIconMaterial"][data-testid="collapsedControl"] {
        color: #ffffff !important;
        opacity: 1 !important;
        display: block !important;
    }
    
    /* 展开按钮 (向左箭头) */
    [data-testid="stIconMaterial"][data-testid="expandedControl"] {
        color: #ffffff !important;
        opacity: 1 !important;
        display: block !important;
    }
    
    /* 确保所有stIconMaterial图标始终可见 */
    [data-testid="stIconMaterial"] {
        color: #ffffff !important;
        opacity: 1 !important;
        display: block !important;
    }
    
    /* 确保侧边栏控制区域始终可见 */
    [data-testid="collapsedControl"], [data-testid="expandedControl"] {
        opacity: 1 !important;
        display: flex !important;
        visibility: visible !important;
    }
    
    /* 确保侧边栏控制按钮容器始终显示 */
    .st-emotion-cache-1v0mbdj,
    .st-emotion-cache-ujm5ma,
    .st-emotion-cache-1nqbn9b {
        opacity: 1 !important;
        display: block !important;
        visibility: visible !important;
    }
    
    /* 覆盖任何悬停相关的样式 */
    .st-emotion-cache-1v0mbdj:hover,
    [data-testid="collapsedControl"]:hover,
    [data-testid="expandedControl"]:hover {
        opacity: 1 !important; /* 保持不透明 */
    }
    
    /* 确保侧边栏隐藏时控制区域也可见 */
    .css-1d391kg {
        visibility: visible !important;
        opacity: 1 !important;
    }
    
    /* 表格样式优化 */
    [data-testid="stDataFrame"], [data-testid="stTable"] {
        font-family: 'Microsoft YaHei', Arial, sans-serif;
        font-size: 14px;
        /* 确保整个表格容器左对齐 */
        display: block !important;
        text-align: left !important;
    }
    
    /* 重点：确保所有页面所有表格的单元格内容靠左对齐 */
    /* 直接针对所有表格单元格内容的样式，最高优先级 */
    [data-testid="stDataFrame"] tbody td,
    [data-testid="stTable"] tbody td,
    [data-testid="columns_table"] tbody td,
    [data-testid="measures_table"] tbody td,
    [data-testid="relationships_table"] tbody td,
    table tbody td {
        text-align: left !important;
        text-align-last: left !important;
        /* 确保文本内容靠左 */
        justify-content: flex-start !important;
        align-items: flex-start !important;
        /* 确保单元格内部元素靠左 */
        display: table-cell !important;
        /* 重置可能的display属性 */
        vertical-align: top !important;
        /* 确保内容从左侧开始 */
        padding-left: 8px !important;
        padding-right: 8px !important;
    }
    
    /* 确保所有页面单元格内所有内容元素靠左 */
    [data-testid="stDataFrame"] td *,
    [data-testid="stTable"] td *,
    [data-testid="columns_table"] td *,
    [data-testid="measures_table"] td *,
    [data-testid="relationships_table"] td *,
    table td * {
        text-align: left !important;
        text-align-last: left !important;
        justify-content: flex-start !important;
        align-items: flex-start !important;
        display: inline !important;
        /* 确保内容元素保持内联状态 */
    }
    
    /* 表格内部所有元素左对齐 - 覆盖所有页面 */
    [data-testid="stDataFrame"] *, 
    [data-testid="stTable"] *, 
    [data-testid="columns_table"] *, 
    [data-testid="measures_table"] *, 
    [data-testid="relationships_table"] *, 
    table * {
        text-align: left !important;
        justify-content: flex-start !important;
        align-items: flex-start !important;
    }
    
    /* 表格主体内容左对齐 - 覆盖所有页面 */
    [data-testid="stDataFrame"] tbody, 
    [data-testid="stTable"] tbody,
    [data-testid="columns_table"] tbody,
    [data-testid="measures_table"] tbody,
    [data-testid="relationships_table"] tbody,
    table tbody {
        text-align: left !important;
    }
    
    [data-testid="stDataFrame"] tbody tr, 
    [data-testid="stTable"] tbody tr,
    [data-testid="columns_table"] tbody tr,
    [data-testid="measures_table"] tbody tr,
    [data-testid="relationships_table"] tbody tr,
    table tbody tr {
        text-align: left !important;
    }
    
    /* 针对Streamlit的表格底层实现 - 覆盖所有页面 */
    .dataframe, 
    .dataframe tbody, 
    .dataframe tr, 
    .dataframe td, 
    .dataframe th {
        text-align: left !important;
        text-align-last: left !important;
    }
    
    /* 针对pandas表格的额外样式 - 覆盖所有页面 */
    .stDataFrame, .stTable {
        text-align: left !important;
    }
    
    .stDataFrame td, .stTable td {
        text-align: left !important;
        text-align-last: left !important;
    }
    
    /* 防止Streamlit默认样式覆盖 - 覆盖所有页面 */
    [data-baseweb="table"] {
        text-align: left !important;
    }
    
    [data-baseweb="table"] td {
        text-align: left !important;
        text-align-last: left !important;
        display: table-cell !important;
        vertical-align: top !important;
    }
    
    [data-baseweb="table"] tbody td {
        text-align: left !important;
        text-align-last: left !important;
        display: table-cell !important;
        vertical-align: top !important;
    }
    
    /* 针对不同页面的特定表格ID */
    [data-testid="columns_table"], 
    [data-testid="measures_table"], 
    [data-testid="relationships_table"] {
        text-align: left !important;
        width: 100% !important;
        display: block !important;
    }
    
    /* 确保所有表格中所有文本内容靠左 */
    [data-testid="stDataFrame"] text,
    [data-testid="stTable"] text,
    [data-testid="columns_table"] text,
    [data-testid="measures_table"] text,
    [data-testid="relationships_table"] text {
        text-anchor: start !important;
        dominant-baseline: hanging !important;
    }
    
    /* 确保Streamlit的内部表格组件靠左 */
    ._StyledTable {
        text-align: left !important;
    }
    
    /* 确保所有数据显示相关组件靠左 */
    .data-table,
    .table-wrapper,
    .streamlit-expanderHeader {
        text-align: left !important;
    }
    
    /* 滚动条样式优化 */
    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }
    
    ::-webkit-scrollbar-track {
        background: #f1f1f1;
        border-radius: 4px;
    }
    
    ::-webkit-scrollbar-thumb {
        background: #c1c1c1;
        border-radius: 4px;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: #a1a1a1;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # 初始化会话状态
    if 'parsed_data' not in st.session_state:
        st.session_state['parsed_data'] = None
    
    # 侧边栏
    with st.sidebar:
        # 文件上传
        uploaded_file = st.file_uploader("📁 上传BI模型文件 (.bim)", type="bim")
        
        # 只保留一个有效的解析按钮
        parse_button = st.button("🚀 开始解析", key="parse_button", help="开始解析上传的模型文件", use_container_width=True)
        
        # 添加TMSL脚本上传按钮
        paste_upload_button = st.button("📋 上传TMSL脚本", key="paste_upload_button", help="通过粘贴方式上传TMSL脚本", use_container_width=True)
    
    # 初始化会话状态
    if "show_paste_dialog" not in st.session_state:
        st.session_state["show_paste_dialog"] = False
    
    # 切换弹窗显示状态
    if paste_upload_button:
        st.session_state["show_paste_dialog"] = True
    
    # 使用Streamlit的容器作为弹窗替代不兼容的dialog功能
    if st.session_state["show_paste_dialog"]:
        # 使用容器模拟对话框效果
        st.markdown("## 📋 上传TMSL脚本")
        st.markdown("---")
        # 添加提示信息，说明这是一个模态编辑区域
        st.info("💡 这是一个模态编辑区域，完成编辑后点击解析或关闭按钮。")
        # 使用容器包裹内容
        # 使用表单来确保按钮点击可以正确处理
        with st.form(key="paste_content_form"):
                st.subheader("TMSL脚本内容编辑区")
                # 提供更大的文本区域以方便编辑
                pasted_content = st.text_area(
                    "请粘贴TMSL脚本内容到此处", 
                    height=500,  # 增加高度提供更好的编辑体验
                    key="pasted_content_area",
                    placeholder="{\n  \"name\": \"SemanticModel\",\n  \"compatibilityLevel\": 1500,\n  ...\n}"
                )
                
                # 添加内容提示信息
                st.info("💡 提示：粘贴完整的TMSL脚本后，可以直接在编辑区进行修改，然后点击解析按钮。")
                
                # 创建表单内的提交按钮
                parse_pasted_button = st.form_submit_button("🚀 解析粘贴内容", use_container_width=True)
                
                # 在表单内使用form_submit_button作为关闭按钮
                close_button = st.form_submit_button("❌ 关闭", use_container_width=True)
                
                # 处理关闭按钮逻辑
                if close_button:
                    st.session_state["show_paste_dialog"] = False
                    st.rerun()
                
                # 处理解析逻辑
                if parse_pasted_button:
                    if pasted_content.strip():
                        try:
                            # 更全面的粘贴内容清理
                            # 1. 移除首尾空白字符
                            cleaned_content = pasted_content.strip()
                            
                            # 2. 移除所有BOM标记
                            if cleaned_content.startswith('\ufeff'):
                                cleaned_content = cleaned_content[1:]
                            
                            # 3. 移除可能存在的前导/尾随垃圾字符
                            # 查找第一个'{'和最后一个'}'来确保只保留JSON部分
                            if '{' in cleaned_content and '}' in cleaned_content:
                                start_idx = cleaned_content.find('{')
                                end_idx = cleaned_content.rfind('}') + 1
                                cleaned_content = cleaned_content[start_idx:end_idx]
                            
                            # 4. 处理可能的空白字符编码问题
                            import re
                            # 移除不可见的控制字符
                            cleaned_content = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', cleaned_content)
                            # 标准化空白字符
                            cleaned_content = re.sub(r'\s+', ' ', cleaned_content)
                            
                            st.info(f"📋 处理后的内容长度: {len(cleaned_content)} 字符")
                            st.info(f"📋 内容开头: {cleaned_content[:50]}...")
                            
                            # 验证内容是否为有效的JSON格式
                            try:
                                json.loads(cleaned_content)
                                st.success("✅ JSON格式验证通过！")
                            except json.JSONDecodeError as je:
                                st.error(f"❌ 无效的JSON格式: {str(je)}")
                                st.info("💡 提示：")
                                st.info("1. 请确保粘贴的是完整的TMSL脚本内容")
                                st.info("2. 检查是否有多余的字符或格式问题")
                                st.info("3. 尝试重新复制文件内容")
                                # 显示更多调试信息
                                if len(cleaned_content) < 500:
                                    st.code(cleaned_content, language="json")
                                return
                            
                            # 解析粘贴的内容
                            parser = BIMParser()
                            result = parser.parse_file(cleaned_content)
                            
                            if result["success"]:
                                st.session_state['parsed_data'] = result
                                st.success("✅ 内容解析成功！")
                                st.session_state["show_paste_dialog"] = False
                                # 强制刷新页面以显示解析结果
                                st.rerun()
                            else:
                                st.error(f"❌ 内容解析失败: {result['error']}")
                                st.info("💡 请检查粘贴的内容是否为有效的TMSL脚本格式")
                        except Exception as e:
                            st.error(f"❌ 处理内容时出错: {str(e)}")
                            st.info("💡 请尝试重新复制完整的模型文件内容")
                            import traceback
                            st.code(traceback.format_exc(), language="python")
                    else:
                        st.warning("⚠️ 请输入有效的模型内容")
        
        st.write("---")
        st.subheader("📋 使用说明")
        st.markdown("""
        - 📁 在上方上传BI模型文件 (.bim)
        - 🚀 点击"开始解析"按钮
        - 📊 在主界面查看解析结果
        - 🔍 使用搜索功能筛选数据
        - 💾 导出需要的格式
        """)
    
    # 主界面
    st.markdown("## 📊 欢迎使用BI模型解析工具")
    
    # 处理文件解析
    if parse_button and uploaded_file is not None:
        try:
            # 读取文件内容
            file_content = uploaded_file.getvalue().decode("utf-8")
            
            # 解析文件
            parser = BIMParser()
            result = parser.parse_file(file_content)
            
            if result["success"]:
                st.session_state['parsed_data'] = result
                st.success("✅ 文件解析成功！")
            else:
                st.error(f"❌ 文件解析失败: {result['error']}")
        except Exception as e:
            st.error(f"❌ 处理文件时出错: {str(e)}")
    
    # 显示解析结果
    if st.session_state['parsed_data'] is not None:
        data = st.session_state['parsed_data']
        
        # 创建标签页
        tab1, tab2, tab3, tab4 = st.tabs([
            "📋 表明细", 
            "📝 列明细", 
            "📈 度量值", 
            "🔗 表关系"
        ])
        
        with tab1:
            overview_df = pd.DataFrame(data['overview'])
            
            if not overview_df.empty:
                # 按表名列字母升序排序
                overview_df = overview_df.sort_values(by='表名', ascending=True)
                # 添加序号列
                overview_df.insert(0, '序号', range(1, len(overview_df) + 1))
                
                # 实时搜索功能 - 无需按回车键，输入时自动搜索
                input_value = st.text_input(
                    "🔍 搜索表名或表描述", 
                    key="table_search_input"
                )
                
                # 直接更新搜索状态，无需等待回车
                update_search_timer("table_search", input_value)
                
                # 获取搜索词
                search_term = debounced_search("table_search")
                
                # 根据搜索词过滤，支持空搜索（显示所有数据）
                if search_term:
                    # 构建搜索条件，确保列存在时才进行搜索
                    search_conditions = []
                    
                    # 表名搜索
                    if '表名' in overview_df.columns:
                        search_conditions.append(overview_df['表名'].str.contains(search_term, case=False, na=False))
                    
                    # 表描述搜索 - 安全处理可能不存在的列
                    if '表描述' in overview_df.columns:
                        search_conditions.append(overview_df['表描述'].str.contains(search_term, case=False, na=False))
                    
                    # 源表名搜索
                    if '源表名' in overview_df.columns:
                        search_conditions.append(overview_df['源表名'].str.contains(search_term, case=False, na=False))
                    
                    # 数据库名搜索
                    if '数据库名' in overview_df.columns:
                        search_conditions.append(overview_df['数据库名'].str.contains(search_term, case=False, na=False))
                    
                    # 只有当有搜索条件时才进行过滤
                    if search_conditions:
                        # 使用逻辑或组合所有条件
                        combined_condition = search_conditions[0]
                        for cond in search_conditions[1:]:
                            combined_condition = combined_condition | cond
                        
                        overview_df = overview_df[combined_condition]
                
                # 计算统计信息
                # 表总数：所有表名的除重计数
                table_count = len(set(overview_df['表名']))
                
                # 列总数：每个表的列名除重计数加总
                # 从columns_info中获取数据
                columns_data = data.get('columns', [])
                column_count = len(columns_data)
                
                # 度量值总数：度量值名称除重计数加总
                measures_data = data.get('measures', [])
                measure_count = len(measures_data)
                
                # 关系条数：表关系的总数
                relationships_data = data.get('relationships', [])
                relationship_count = len(relationships_data)
                
                # 显示概览信息
                st.info(f"📊 统计信息: 表总数 {table_count} 个, 列总数 {column_count} 个, 度量值总数 {measure_count} 个, 关系条数 {relationship_count} 个")
                
                # 配置列的宽度和类型
                column_configs = {}
                for col in overview_df.columns:
                    if col == '序号':
                        # 序号列配置为数字类型，确保正确排序
                        column_configs[col] = st.column_config.NumberColumn(
                            col,
                            width="small"
                        )
                    elif col in ['表名', '表描述', '实例地址']:
                        column_configs[col] = st.column_config.TextColumn(
                            col,
                            width="medium"
                        )
                    elif col in ['分区数', '行数', '协议']:
                        column_configs[col] = st.column_config.TextColumn(
                            col,
                            width="small"
                        )
                    elif col in ['数据库名', '源表名']:
                        column_configs[col] = st.column_config.TextColumn(
                            col,
                            width="medium"
                        )
                    else:
                        column_configs[col] = st.column_config.TextColumn(
                            col,
                            width="small"
                        )
                
                # 设置表格高度 - 自适应行数，只有超过15行时才需要滚动
                max_rows_without_scroll = 15  # 不滚动可显示的最大行数
                row_height = 35  # 每行高度
                header_height = 50  # 表头高度
                
                if len(overview_df) <= max_rows_without_scroll:
                    # 如果行数较少，完全自适应显示
                    table_height = len(overview_df) * row_height + header_height
                else:
                    # 超过最大行数时，设置最大高度
                    table_height = max_rows_without_scroll * row_height + header_height
                
                # 显示表格
                st.dataframe(
                    overview_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config=column_configs,
                    key="overview_table",
                    height=table_height
                )
            else:
                st.warning("⚠️ 没有找到概览数据")
        
        with tab2:
            columns_df = pd.DataFrame(data['columns'])
            
            if not columns_df.empty:
                # 按表名列字母升序排序
                columns_df = columns_df.sort_values(by='表名', ascending=True)
                # 添加序号列
                columns_df.insert(0, '序号', range(1, len(columns_df) + 1))
                
                # 实时搜索功能 - 使用防抖优化性能
                input_value = st.text_input(
                    "🔍 搜索表名、列名或源列名", 
                    key="column_search_input",
                    on_change=lambda: update_search_timer("column_search", st.session_state.column_search_input)
                )
                
                # 初始化时也需要更新一次定时器
                update_search_timer("column_search", input_value)
                
                # 获取经过防抖处理的搜索词
                debounced_term = debounced_search("column_search")
                
                # 如果有防抖处理后的搜索词，则执行搜索
                if debounced_term:
                    # 构建搜索条件，确保列存在时才进行搜索
                    search_conditions = []
                    
                    # 表名搜索
                    if '表名' in columns_df.columns:
                        search_conditions.append(columns_df['表名'].str.contains(debounced_term, case=False, na=False))
                    
                    # 列名搜索
                    if '列名' in columns_df.columns:
                        search_conditions.append(columns_df['列名'].str.contains(debounced_term, case=False, na=False))
                    
                    # 源列名搜索
                    if '源列名' in columns_df.columns:
                        search_conditions.append(columns_df['源列名'].str.contains(debounced_term, case=False, na=False))
                    
                    # 只有当有搜索条件时才进行过滤
                    if search_conditions:
                        # 使用逻辑或组合所有条件
                        combined_condition = search_conditions[0]
                        for cond in search_conditions[1:]:
                            combined_condition = combined_condition | cond
                        
                        columns_df = columns_df[combined_condition]
                
                # 显示筛选结果数量
                st.info(f"📝 共显示 {len(columns_df)} 条列记录")
                
                # 设置表格高度 - 自适应行数，只有超过15行时才需要滚动
                max_rows_without_scroll = 15  # 不滚动可显示的最大行数
                row_height = 35  # 每行高度
                header_height = 50  # 表头高度
                
                if len(columns_df) <= max_rows_without_scroll:
                    # 如果行数较少，完全自适应显示
                    table_height = len(columns_df) * row_height + header_height
                else:
                    # 超过最大行数时，设置最大高度
                    table_height = max_rows_without_scroll * row_height + header_height
                
                # 配置列的宽度和类型
                column_configs = {}
                for col in columns_df.columns:
                    if col == '序号':
                        # 序号列配置为数字类型，确保正确排序
                        column_configs[col] = st.column_config.NumberColumn(
                            col,
                            width="small"
                        )
                    else:
                        column_configs[col] = st.column_config.TextColumn(
                            col,
                            width="medium"
                        )
                
                # 显示表格
                st.dataframe(
                    columns_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config=column_configs,
                    key="columns_table",
                    height=table_height
                )
            else:
                st.warning("⚠️ 没有找到列数据")
        
        with tab3:
            measures_df = pd.DataFrame(data['measures'])
            
            if not measures_df.empty:
                # 按度量值涉及表列字母升序排序
                measures_df = measures_df.sort_values(by='度量值涉及表', ascending=True)
                # 添加序号列
                measures_df.insert(0, '序号', range(1, len(measures_df) + 1))
                
                # 实时搜索功能 - 无需按回车键，输入时自动搜索
                input_value = st.text_input(
                    "🔍 搜索度量值名称或计算逻辑", 
                    key="measure_search_input"
                )
                
                # 直接更新搜索状态，无需等待回车
                update_search_timer("measure_search", input_value)
                
                # 获取搜索词
                search_term = debounced_search("measure_search")
                
                # 根据搜索词过滤，支持空搜索（显示所有数据）
                if search_term:
                    # 构建搜索条件，确保列存在时才进行搜索
                    search_conditions = []
                    
                    # 度量值名称搜索
                    if '度量值名称' in measures_df.columns:
                        search_conditions.append(measures_df['度量值名称'].str.contains(search_term, case=False, na=False))
                    
                    # 度量值计算逻辑搜索
                    if '度量值计算逻辑' in measures_df.columns:
                        search_conditions.append(measures_df['度量值计算逻辑'].str.contains(search_term, case=False, na=False))
                    
                    # 只有当有搜索条件时才进行过滤
                    if search_conditions:
                        # 使用逻辑或组合所有条件
                        combined_condition = search_conditions[0]
                        for cond in search_conditions[1:]:
                            combined_condition = combined_condition | cond
                        
                        measures_df = measures_df[combined_condition]
                
                # 显示筛选结果数量
                st.info(f"📈 共显示 {len(measures_df)} 条度量值记录")
                
                # 设置表格高度 - 自适应行数，只有超过15行时才需要滚动
                max_rows_without_scroll = 15  # 不滚动可显示的最大行数
                row_height = 35  # 每行高度
                header_height = 50  # 表头高度
                
                if len(measures_df) <= max_rows_without_scroll:
                    # 如果行数较少，完全自适应显示
                    table_height = len(measures_df) * row_height + header_height
                else:
                    # 超过最大行数时，设置最大高度
                    table_height = max_rows_without_scroll * row_height + header_height
                
                # 配置列的宽度和类型
                column_configs = {}
                for col in measures_df.columns:
                    if col == '序号':
                        # 序号列配置为数字类型，确保正确排序
                        column_configs[col] = st.column_config.NumberColumn(
                            col,
                            width="small"
                        )
                    else:
                        column_configs[col] = st.column_config.TextColumn(
                            col,
                            width="medium"
                        )
                
                # 显示表格
                st.dataframe(
                    measures_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config=column_configs,
                    key="measures_table",
                    height=table_height
                )
            else:
                st.warning("⚠️ 没有找到度量值数据")
        
        with tab4:
            relationships_df = pd.DataFrame(data['relationships'])
            
            if not relationships_df.empty:
                # 按源表名列字母升序排序
                relationships_df = relationships_df.sort_values(by='源表名', ascending=True)
                # 添加序号列
                relationships_df.insert(0, '序号', range(1, len(relationships_df) + 1))
                
                # 实时搜索功能 - 无需按回车键，输入时自动搜索
                input_value = st.text_input(
                    "🔍 搜索表名或字段名", 
                    key="relationship_search_input"
                )
                
                # 直接更新搜索状态，无需等待回车
                update_search_timer("relationship_search", input_value)
                
                # 获取搜索词
                search_term = debounced_search("relationship_search")
                
                # 根据搜索词过滤，支持空搜索（显示所有数据）
                if search_term:
                    # 构建搜索条件，确保列存在时才进行搜索
                    search_conditions = []
                    
                    # 源表名搜索
                    if '源表名' in relationships_df.columns:
                        search_conditions.append(relationships_df['源表名'].str.contains(search_term, case=False, na=False))
                    
                    # 目标表名搜索
                    if '目标表名' in relationships_df.columns:
                        search_conditions.append(relationships_df['目标表名'].str.contains(search_term, case=False, na=False))
                    
                    # 源表字段搜索
                    if '源表字段' in relationships_df.columns:
                        search_conditions.append(relationships_df['源表字段'].str.contains(search_term, case=False, na=False))
                    
                    # 目标表字段搜索
                    if '目标表字段' in relationships_df.columns:
                        search_conditions.append(relationships_df['目标表字段'].str.contains(search_term, case=False, na=False))
                    
                    # 只有当有搜索条件时才进行过滤
                    if search_conditions:
                        # 使用逻辑或组合所有条件
                        combined_condition = search_conditions[0]
                        for cond in search_conditions[1:]:
                            combined_condition = combined_condition | cond
                        
                        relationships_df = relationships_df[combined_condition]
                
                # 显示筛选结果数量
                st.info(f"🔗 共显示 {len(relationships_df)} 条关系记录")
                
                # 设置表格高度 - 自适应行数，只有超过15行时才需要滚动
                max_rows_without_scroll = 15  # 不滚动可显示的最大行数
                row_height = 35  # 每行高度
                header_height = 50  # 表头高度
                
                if len(relationships_df) <= max_rows_without_scroll:
                    # 如果行数较少，完全自适应显示
                    table_height = len(relationships_df) * row_height + header_height
                else:
                    # 超过最大行数时，设置最大高度
                    table_height = max_rows_without_scroll * row_height + header_height
                
                # 配置列的宽度和类型
                column_configs = {}
                for col in relationships_df.columns:
                    if col == '序号':
                        # 序号列配置为数字类型，确保正确排序
                        column_configs[col] = st.column_config.NumberColumn(
                            col,
                            width="small"
                        )
                    else:
                        column_configs[col] = st.column_config.TextColumn(
                            col,
                            width="medium"
                        )
                
                # 显示表格
                st.dataframe(
                    relationships_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config=column_configs,
                    key="relationships_table",
                    height=table_height
                )
            else:
                st.warning("⚠️ 没有找到关系数据")
            
            # 恢复导出功能 - 添加到侧边栏，并优化样式
            with st.sidebar.expander("📤 数据导出", expanded=False):
                # 添加CSS样式优化布局和字体大小
                st.markdown(
                    """
                    <style>
                        .export-sidebar * {
                            font-size: 0.75rem !important;
                            margin-bottom: 0 !important;
                            margin-top: 0 !important;
                            line-height: 1.1;
                        }
                        .export-sidebar .stVerticalBlock {
                            gap: 0 !important;
                        }
                        .export-sidebar .stButton button {
                            height: 28px !important;
                            padding: 0.1rem 0.3rem !important;
                            margin-top: 0 !important;
                            margin-bottom: 0 !important;
                        }
                        .export-sidebar .stAlert {
                            margin-top: 0 !important;
                            margin-bottom: 0 !important;
                            padding: 0.2rem !important;
                            font-size: 0.7rem !important;
                        }
                        .export-sidebar .stSelectbox {
                            margin-bottom: 0 !important;
                            margin-top: 0 !important;
                            padding: 0 !important;
                        }
                        .export-sidebar h3 {
                            font-size: 0.7rem !important;
                            margin-bottom: 0 !important;
                        }
                        .export-sidebar {
                            font-size: 0.75rem !important;
                            margin-top: -15px !important;
                            padding: 0 !important;
                            line-height: 1.0 !important;
                        }
                        .export-sidebar > *:first-child {
                            margin-top: 0 !important;
                        }
                        .export-sidebar .stLabel {
                            margin-bottom: 0 !important;
                            margin-top: 0 !important;
                        }
                        .export-sidebar .stMarkdown {
                            margin-bottom: 0 !important;
                            margin-top: 0 !important;
                        }
                        .export-sidebar .stSelectbox div[data-baseweb="select"] {
                            margin-top: 0 !important;
                            margin-bottom: 0 !important;
                        }
                        .export-sidebar .stButton {
                            margin-top: 0 !important;
                            margin-bottom: 0 !important;
                        }
                    </style>
                    """,
                    unsafe_allow_html=True
                )
                
                st.markdown('<div class="export-sidebar">', unsafe_allow_html=True)
                # 选择要导出的数据类型 - 与页面显示保持一致
                export_type = st.selectbox(
                    "选择数据类型",
                    ["表明细", "列明细", "度量值", "表关系", "全部导出"],
                    index=4  # 默认选择"全部导出"
                )
                # 选择导出格式
                export_format = st.selectbox(
                    "选择导出格式",
                    ["CSV", "Excel"],
                    index=1  # 默认选择"Excel"
                )
                
                # 添加隐藏的会话状态变量用于跟踪下载状态
                if "export_data" not in st.session_state:
                    st.session_state.export_data = None
                if "export_filename" not in st.session_state:
                    st.session_state.export_filename = None
                if "export_mime" not in st.session_state:
                    st.session_state.export_mime = None
                if "export_ready" not in st.session_state:
                    st.session_state.export_ready = False
                
                # 导出按钮：较为稳健的实现，Excel 尽量延迟使用 openpyxl，否则回退为 CSV ZIP
                if st.button("开始导出", use_container_width=True):
                    try:
                        if export_type == "全部导出":
                            sheet_data = [
                                ('表明细', data['overview']),
                                ('列明细', data['columns']),
                                ('度量值', data['measures']),
                                ('表关系', data['relationships'])
                            ]

                            # 如果用户选择 Excel，但 openpyxl 不可用 -> 回退为 CSV ZIP
                            if export_format == 'Excel' and not _OPENPYXL_AVAILABLE:
                                st.warning("当前环境未安装 openpyxl，已退回为 CSV 压缩包导出。若需 Excel 输出，请安装 openpyxl 并重试。")
                                export_format = 'CSV'

                            if export_format == 'CSV':
                                zip_buffer = io.BytesIO()
                                with zipfile.ZipFile(zip_buffer, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
                                    for sheet_name, sheet_rows in sheet_data:
                                        df_sheet = pd.DataFrame(sheet_rows)
                                        csv_bytes = df_sheet.to_csv(index=False).encode('utf-8')
                                        zf.writestr(f"{sheet_name}.csv", csv_bytes)

                                zip_buffer.seek(0)
                                filename = f"bi_model_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
                                st.success("✅ 数据准备完成，点击下方按钮下载")
                                st.download_button("⬇️ 下载 ZIP (CSV)", data=zip_buffer.getvalue(), file_name=filename, mime='application/zip')
                            else:
                                output = io.BytesIO()
                                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                                    for sheet_name, sheet_rows in sheet_data:
                                        df_sheet = pd.DataFrame(sheet_rows)
                                        df_sheet.to_excel(writer, sheet_name=sheet_name[:31], index=False)

                                output.seek(0)
                                filename = f"bi_model_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                                st.success("✅ 数据准备完成，点击下方按钮下载")
                                st.download_button("⬇️ 下载 Excel", data=output.getvalue(), file_name=filename, mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                                st.markdown("<style>.stSidebar [data-testid='stVerticalBlock'] {gap: 0.2rem;}</style>", unsafe_allow_html=True)
                        else:
                            # 导出特定类型（表明细/列明细/度量值/表关系）
                            if export_type == "表明细":
                                export_df = pd.DataFrame(data['overview'])
                                export_df.insert(0, '序号', range(1, len(export_df) + 1))
                                file_name = f"表明细_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                            elif export_type == "列明细":
                                export_df = pd.DataFrame(data['columns'])
                                export_df.insert(0, '序号', range(1, len(export_df) + 1))
                                if 'column_search' in st.session_state and st.session_state['column_search']:
                                    search_term = st.session_state['column_search']
                                    export_df = export_df[
                                        export_df['表名'].str.contains(search_term, case=False, na=False) |
                                        export_df['列名'].str.contains(search_term, case=False, na=False) |
                                        export_df['源列名'].str.contains(search_term, case=False, na=False) |
                                        export_df.get('列描述', '').str.contains(search_term, case=False, na=False)
                                    ]
                                file_name = f"列明细_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                            elif export_type == "度量值":
                                export_df = pd.DataFrame(data['measures'])
                                export_df.insert(0, '序号', range(1, len(export_df) + 1))
                                if 'measure_search' in st.session_state and st.session_state['measure_search']:
                                    search_term = st.session_state['measure_search']
                                    export_df = export_df[
                                        export_df['度量值名称'].str.contains(search_term, case=False, na=False) |
                                        export_df['度量值计算逻辑'].str.contains(search_term, case=False, na=False) |
                                        export_df['度量值涉及表'].str.contains(search_term, case=False, na=False)
                                    ]
                                file_name = f"度量值_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                            elif export_type == "表关系":
                                export_df = pd.DataFrame(data['relationships'])
                                export_df.insert(0, '序号', range(1, len(export_df) + 1))
                                if 'relationship_search' in st.session_state and st.session_state['relationship_search']:
                                    search_term = st.session_state['relationship_search']
                                    export_df = export_df[
                                        export_df['源表名'].str.contains(search_term, case=False, na=False) |
                                        export_df['目标表名'].str.contains(search_term, case=False, na=False) |
                                        export_df['源表字段'].str.contains(search_term, case=False, na=False) |
                                        export_df['目标表字段'].str.contains(search_term, case=False, na=False)
                                    ]
                                file_name = f"表关系_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

                            # 输出为 CSV 或 Excel
                            if export_format == "CSV":
                                csv_data = export_df.to_csv(index=False, encoding='utf-8-sig')
                                st.session_state.export_data = csv_data
                                st.session_state.export_filename = f"{file_name}.csv"
                                st.session_state.export_mime = "text/csv"
                                st.session_state.export_ready = True
                            else:
                                if not _OPENPYXL_AVAILABLE:
                                    st.error("导出 Excel 需要安装 openpyxl。请在运行环境中安装后重试。")
                                    raise RuntimeError("openpyxl 不可用")

                                output = io.BytesIO()
                                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                                    export_df.to_excel(writer, index=False)

                                output.seek(0)
                                st.session_state.export_data = output.getvalue()
                                st.session_state.export_filename = f"{file_name}.xlsx"
                                st.session_state.export_mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                st.session_state.export_ready = True

                            # 显示下载按钮
                            st.success("✅ 数据准备完成，点击下方按钮下载")
                            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                            if export_format == "CSV":
                                st.download_button(
                                    label=f"下载 {export_type}.csv",
                                    data=st.session_state.export_data,
                                    file_name=f"BI模型解析数据_{export_type}_{timestamp}.csv",
                                    mime="text/csv",
                                    use_container_width=True,
                                    key=f"download_csv_{datetime.now().timestamp()}"
                                )
                            else:
                                st.download_button(
                                    label=f"下载 {export_type}.xlsx",
                                    data=st.session_state.export_data,
                                    file_name=f"BI模型解析数据_{export_type}_{timestamp}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    use_container_width=True,
                                    key=f"download_excel_{datetime.now().timestamp()}"
                                )
                                st.markdown("<style>.stSidebar [data-testid='stVerticalBlock'] {gap: 0.2rem;}</style>", unsafe_allow_html=True)
                    except Exception as e:
                        st.error(f"❌ 导出失败: {str(e)}")

                # 简化的导出流程 - 使用Streamlit原生下载按钮
                st.markdown('</div>', unsafe_allow_html=True)  # 关闭样式容器
    else:
        # 欢迎界面 - 使用Streamlit原生组件替代HTML
        st.container()
        col1, col2, col3 = st.columns([1, 3, 1])
        with col2:
            st.subheader("✨ 主要功能")
            st.markdown("""
            - ✅ 解析BI模型结构信息
            - ✅ 智能搜索和筛选
            - ✅ 数据导出
            """)
            
            st.subheader("📚 使用教程")
            st.markdown("""
            - 📁 在左侧上传BI模型.BIM文件或粘贴TMSL脚本
            - 🚀 点击"开始解析"按钮
            - 📊 在各个标签页查看解析结果
            - 🔍 使用搜索功能快速定位
            - 💾 导出需要的格式
            """)
            
            st.error("📢 请在左侧上传您的BI模型文件！")

if __name__ == "__main__":
    create_streamlit_app()