@echo off
REM ============================================================================
REM Kuber Shutdown Utility for Windows
REM Copyright © 2025-2030, All Rights Reserved
REM Ashutosh Sinha | Email: ajsinha@gmail.com
REM ============================================================================
REM
REM This script triggers a graceful shutdown of the Kuber cache server.
REM
REM Usage:
REM   kuber-shutdown.bat [options]
REM
REM Options:
REM   /file              Use file-based shutdown (default)
REM   /api               Use REST API shutdown
REM   /key:KEY           API key for REST API shutdown
REM   /host:HOST         API host (default: localhost)
REM   /port:PORT         API port (default: 8080)
REM   /dir:DIR           Directory containing kuber.shutdown file
REM   /reason:TEXT       Shutdown reason (for logging)
REM   /help              Show this help message
REM
REM Examples:
REM   kuber-shutdown.bat                           - File-based shutdown
REM   kuber-shutdown.bat /api /key:myapikey        - API-based shutdown
REM   kuber-shutdown.bat /dir:C:\kuber             - Custom directory
REM   kuber-shutdown.bat /reason:"Maintenance"     - With reason
REM
REM ============================================================================

setlocal enabledelayedexpansion

REM Default values
set SHUTDOWN_METHOD=file
set API_HOST=localhost
set API_PORT=8080
set API_KEY=
set SHUTDOWN_DIR=.
set SHUTDOWN_FILE=kuber.shutdown
set REASON=Manual shutdown

REM Parse arguments
:parse_args
if "%~1"=="" goto :done_parsing
if /i "%~1"=="/file" (
    set SHUTDOWN_METHOD=file
    shift
    goto :parse_args
)
if /i "%~1"=="/api" (
    set SHUTDOWN_METHOD=api
    shift
    goto :parse_args
)
if /i "%~1:~0,5%"=="/key:" (
    set API_KEY=%~1
    set API_KEY=!API_KEY:~5!
    shift
    goto :parse_args
)
if /i "%~1:~0,6%"=="/host:" (
    set API_HOST=%~1
    set API_HOST=!API_HOST:~6!
    shift
    goto :parse_args
)
if /i "%~1:~0,6%"=="/port:" (
    set API_PORT=%~1
    set API_PORT=!API_PORT:~6!
    shift
    goto :parse_args
)
if /i "%~1:~0,5%"=="/dir:" (
    set SHUTDOWN_DIR=%~1
    set SHUTDOWN_DIR=!SHUTDOWN_DIR:~5!
    shift
    goto :parse_args
)
if /i "%~1:~0,8%"=="/reason:" (
    set REASON=%~1
    set REASON=!REASON:~8!
    shift
    goto :parse_args
)
if /i "%~1"=="/help" (
    goto :show_help
)
echo Unknown option: %~1
goto :show_help

:done_parsing

REM Print banner
echo.
echo ========================================================================
echo                     KUBER SHUTDOWN UTILITY
echo                         Version 1.3.9
echo ========================================================================
echo.

if "%SHUTDOWN_METHOD%"=="file" goto :file_shutdown
if "%SHUTDOWN_METHOD%"=="api" goto :api_shutdown
echo Unknown shutdown method: %SHUTDOWN_METHOD%
exit /b 1

:file_shutdown
set SHUTDOWN_PATH=%SHUTDOWN_DIR%\%SHUTDOWN_FILE%

echo Method: File-based shutdown
echo Creating shutdown file: %SHUTDOWN_PATH%
echo.

REM Create the shutdown file with reason
echo Shutdown requested at %date% %time% > "%SHUTDOWN_PATH%"
echo Reason: %REASON% >> "%SHUTDOWN_PATH%"

if exist "%SHUTDOWN_PATH%" (
    echo [SUCCESS] Shutdown file created successfully
    echo.
    echo Kuber will detect the file within 5 seconds and begin graceful shutdown.
    echo The shutdown process takes approximately 30 seconds to complete.
    echo.
    echo Monitor the application logs for shutdown progress.
) else (
    echo [ERROR] Failed to create shutdown file
    exit /b 1
)
goto :end

:api_shutdown
if "%API_KEY%"=="" (
    echo [ERROR] API key required for API-based shutdown
    echo Use: kuber-shutdown.bat /api /key:YOUR_API_KEY
    exit /b 1
)

set API_URL=http://%API_HOST%:%API_PORT%/api/admin/shutdown

echo Method: REST API shutdown
echo API URL: %API_URL%
echo.

REM Check if curl is available
where curl >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo [ERROR] curl is required for API-based shutdown
    echo Please install curl or use file-based shutdown
    exit /b 1
)

REM Make the API call
curl -s -X POST "%API_URL%?reason=%REASON%" -H "X-API-Key: %API_KEY%" -H "Content-Type: application/json"

if %ERRORLEVEL% equ 0 (
    echo.
    echo [SUCCESS] Shutdown request sent
    echo.
    echo Kuber is now shutting down gracefully.
    echo The shutdown process takes approximately 30 seconds to complete.
) else (
    echo.
    echo [ERROR] Shutdown request failed
    exit /b 1
)
goto :end

:show_help
echo.
echo ========================================================================
echo                     KUBER SHUTDOWN UTILITY
echo                         Version 1.3.9
echo ========================================================================
echo.
echo Usage: kuber-shutdown.bat [options]
echo.
echo Options:
echo   /file              Use file-based shutdown (default)
echo   /api               Use REST API shutdown
echo   /key:KEY           API key for REST API shutdown
echo   /host:HOST         API host (default: localhost)
echo   /port:PORT         API port (default: 8080)
echo   /dir:DIR           Directory containing kuber.shutdown file
echo   /reason:TEXT       Shutdown reason (for logging)
echo   /help              Show this help message
echo.
echo Examples:
echo   kuber-shutdown.bat                           - File-based shutdown
echo   kuber-shutdown.bat /api /key:myapikey        - API-based shutdown
echo   kuber-shutdown.bat /dir:C:\kuber             - Custom directory
echo   kuber-shutdown.bat /reason:"Maintenance"     - With reason
echo.
exit /b 0

:end
echo.
echo Shutdown initiated. Goodbye!
endlocal
