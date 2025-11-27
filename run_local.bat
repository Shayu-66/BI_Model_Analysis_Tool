@echo off
REM 切换到脚本所在目录（仓库根）
cd /d "%~dp0"

REM 如果需要激活虚拟环境，请取消下面三行注释并根据实际路径修改（可选）
REM echo 正在尝试激活虚拟环境...
REM call "venv\Scripts\activate.bat"

echo 正在启动 Streamlit 服务...
REM 在新命令窗口中启动 Streamlit（保持窗口打开以便查看日志）
start "Streamlit" cmd /k "python -m streamlit run streamlit_app.py --server.port 8501 --server.headless true"

REM 等待服务启动（可根据需要调整等待时间）
timeout /t 4 /nobreak >nul

REM 在默认浏览器打开本地页面
start "" "http://localhost:8501"

exit /b
