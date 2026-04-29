@echo off
REM ============================================================
REM  configurar_task_scheduler.bat
REM  Registra pipeline_completo.py como tarea nocturna en
REM  Windows Task Scheduler (ejecutar como Administrador)
REM ============================================================

SET TASK_NAME=ETL_Gasolineras_Nocturno
SET PYTHON_EXE=C:\Users\%USERNAME%\AppData\Local\Programs\Python\Python311\python.exe
SET SCRIPT_DIR=%~dp0
SET SCRIPT="%SCRIPT_DIR%pipeline_completo.py"
SET LOG_DIR=%SCRIPT_DIR%logs
SET HORA_EJECUCION=02:00

REM Crear carpeta de logs si no existe
IF NOT EXIST "%LOG_DIR%" mkdir "%LOG_DIR%"

REM Eliminar tarea previa si existe (ignora error si no existe)
schtasks /delete /tn "%TASK_NAME%" /f 2>nul

REM Crear la tarea
schtasks /create ^
  /tn "%TASK_NAME%" ^
  /tr "\"%PYTHON_EXE%\" %SCRIPT% --skip-xml >> \"%LOG_DIR%\etl_%%DATE:~-4,4%%%%DATE:~-10,2%%%%DATE:~-7,2%%.log\" 2>&1" ^
  /sc DAILY ^
  /st %HORA_EJECUCION% ^
  /ru "%USERNAME%" ^
  /rl HIGHEST ^
  /f

IF %ERRORLEVEL% EQU 0 (
    echo.
    echo  [OK] Tarea creada: %TASK_NAME%
    echo       Ejecucion diaria a las %HORA_EJECUCION%
    echo       Logs en: %LOG_DIR%
    echo.
    echo  Para verificar:
    echo    schtasks /query /tn "%TASK_NAME%" /fo LIST /v
    echo.
    echo  Para ejecutar manualmente ahora:
    echo    schtasks /run /tn "%TASK_NAME%"
) ELSE (
    echo.
    echo  [ERROR] No se pudo crear la tarea. Ejecuta este script como Administrador.
)

pause