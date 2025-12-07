@echo off
REM ============================================================================
REM Kuber Startup Utility for Windows
REM Copyright © 2025-2030, All Rights Reserved
REM Ashutosh Sinha | Email: ajsinha@gmail.com
REM ============================================================================
REM
REM This script starts the Kuber cache server with configurable options.
REM
REM Usage:
REM   kuber-start.bat [options]
REM
REM Options:
REM   /jar:PATH           Path to kuber-server JAR file
REM   /config:PATH        Path to application.yml config file
REM   /profile:NAME       Spring profile (dev, prod, test)
REM   /memory:SIZE        JVM heap size (default: 2g)
REM   /redis-port:PORT    Redis protocol port (default: 6380)
REM   /http-port:PORT     HTTP/REST port (default: 8080)
REM   /debug              Enable remote debugging on port 5005
REM   /help               Show this help message
REM
REM Examples:
REM   kuber-start.bat                                    - Start with defaults
REM   kuber-start.bat /memory:4g /profile:prod           - 4GB heap, prod profile
REM   kuber-start.bat /redis-port:6381 /http-port:8081   - Custom ports
REM
REM ============================================================================

setlocal enabledelayedexpansion

REM Default values
set JAR_PATH=
set CONFIG_PATH=
set PROFILE=
set MEMORY=2g
set REDIS_PORT=
set HTTP_PORT=
set DEBUG=false

REM Script directory
set SCRIPT_DIR=%~dp0
set BASE_DIR=%SCRIPT_DIR%..

REM Parse arguments
:parse_args
if "%~1"=="" goto :done_parsing
if /i "%~1"=="/help" goto :show_help
if /i "%~1:~0,5%"=="/jar:" (
    set JAR_PATH=%~1
    set JAR_PATH=!JAR_PATH:~5!
    shift
    goto :parse_args
)
if /i "%~1:~0,8%"=="/config:" (
    set CONFIG_PATH=%~1
    set CONFIG_PATH=!CONFIG_PATH:~8!
    shift
    goto :parse_args
)
if /i "%~1:~0,9%"=="/profile:" (
    set PROFILE=%~1
    set PROFILE=!PROFILE:~9!
    shift
    goto :parse_args
)
if /i "%~1:~0,8%"=="/memory:" (
    set MEMORY=%~1
    set MEMORY=!MEMORY:~8!
    shift
    goto :parse_args
)
if /i "%~1:~0,12%"=="/redis-port:" (
    set REDIS_PORT=%~1
    set REDIS_PORT=!REDIS_PORT:~12!
    shift
    goto :parse_args
)
if /i "%~1:~0,11%"=="/http-port:" (
    set HTTP_PORT=%~1
    set HTTP_PORT=!HTTP_PORT:~11!
    shift
    goto :parse_args
)
if /i "%~1"=="/debug" (
    set DEBUG=true
    shift
    goto :parse_args
)
echo Unknown option: %~1
goto :show_help

:done_parsing

REM Print banner
echo.
echo ========================================================================
echo                      KUBER STARTUP UTILITY
echo                         Version 1.4.0
echo ========================================================================
echo.

REM Check Java
where java >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Java is not installed or not in PATH
    echo Please install Java 17 or later
    exit /b 1
)

for /f "tokens=3" %%g in ('java -version 2^>^&1 ^| findstr /i "version"') do (
    set JAVA_VERSION=%%g
)
set JAVA_VERSION=%JAVA_VERSION:"=%
echo Java version: %JAVA_VERSION%

REM Find JAR file
if defined JAR_PATH (
    if exist "%JAR_PATH%" goto :jar_found
)

REM Search in common locations
for %%f in ("%BASE_DIR%\kuber-server\target\kuber-server-*.jar") do (
    if exist "%%f" (
        set JAR_PATH=%%f
        goto :jar_found
    )
)
for %%f in ("%BASE_DIR%\target\kuber-server-*.jar") do (
    if exist "%%f" (
        set JAR_PATH=%%f
        goto :jar_found
    )
)
for %%f in ("%BASE_DIR%\kuber-server-*.jar") do (
    if exist "%%f" (
        set JAR_PATH=%%f
        goto :jar_found
    )
)
for %%f in (".\kuber-server-*.jar") do (
    if exist "%%f" (
        set JAR_PATH=%%f
        goto :jar_found
    )
)

echo [ERROR] Could not find kuber-server JAR file
echo Please build the project first: mvn clean package -DskipTests
echo Or specify the JAR path: kuber-start.bat /jar:path\to\kuber-server.jar
exit /b 1

:jar_found
echo JAR file: %JAR_PATH%

REM Build JVM options
set JVM_OPTS=-Xms%MEMORY% -Xmx%MEMORY%
set JVM_OPTS=%JVM_OPTS% -XX:+UseG1GC
set JVM_OPTS=%JVM_OPTS% -XX:+HeapDumpOnOutOfMemoryError
set JVM_OPTS=%JVM_OPTS% -Djava.awt.headless=true
set JVM_OPTS=%JVM_OPTS% -Dfile.encoding=UTF-8

REM Debug options
if "%DEBUG%"=="true" (
    set JVM_OPTS=%JVM_OPTS% -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005
    echo Debug mode enabled on port 5005
)

REM Build Spring options
set SPRING_OPTS=

if defined CONFIG_PATH (
    set SPRING_OPTS=%SPRING_OPTS% --spring.config.location=%CONFIG_PATH%
    echo Config file: %CONFIG_PATH%
)

if defined PROFILE (
    set SPRING_OPTS=%SPRING_OPTS% --spring.profiles.active=%PROFILE%
    echo Profile: %PROFILE%
)

if defined REDIS_PORT (
    set SPRING_OPTS=%SPRING_OPTS% --kuber.network.redis-port=%REDIS_PORT%
    echo Redis port: %REDIS_PORT%
)

if defined HTTP_PORT (
    set SPRING_OPTS=%SPRING_OPTS% --server.port=%HTTP_PORT%
    echo HTTP port: %HTTP_PORT%
)

echo.
echo [SUCCESS] Starting Kuber...
echo Press Ctrl+C to stop.
echo.

REM Start the server
java %JVM_OPTS% -jar "%JAR_PATH%" %SPRING_OPTS%
goto :end

:show_help
echo.
echo ========================================================================
echo                      KUBER STARTUP UTILITY
echo                         Version 1.4.0
echo ========================================================================
echo.
echo Usage: kuber-start.bat [options]
echo.
echo Options:
echo   /jar:PATH           Path to kuber-server JAR file
echo   /config:PATH        Path to application.yml config file
echo   /profile:NAME       Spring profile (dev, prod, test)
echo   /memory:SIZE        JVM heap size (default: 2g)
echo   /redis-port:PORT    Redis protocol port (default: 6380)
echo   /http-port:PORT     HTTP/REST port (default: 8080)
echo   /debug              Enable remote debugging on port 5005
echo   /help               Show this help message
echo.
echo Examples:
echo   kuber-start.bat                                    - Start with defaults
echo   kuber-start.bat /memory:4g /profile:prod           - 4GB heap, prod profile
echo   kuber-start.bat /redis-port:6381 /http-port:8081   - Custom ports
echo.
exit /b 0

:end
endlocal
