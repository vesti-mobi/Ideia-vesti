@echo off
REM ============================================================
REM  Atualiza dados do dashboard e faz push para o GitHub
REM ============================================================

cd /d "C:\Users\adria\AppData\Local\Temp\Ideia-vesti\CS-Sucesso-do-cliente"

echo [%date% %time%] Iniciando atualizacao... >> atualizar.log

REM 1. Build dos dados principais (Fabric + HubSpot + CSV + Excel)
echo [%date% %time%] Executando build-cloud.js... >> atualizar.log
node build-cloud.js >> atualizar.log 2>&1
if %errorlevel% neq 0 (
    echo [%date% %time%] ERRO ao executar build-cloud.js, tentando build-data.js... >> atualizar.log
    node build-data.js >> atualizar.log 2>&1
    if %errorlevel% neq 0 (
        echo [%date% %time%] ERRO ao executar build-data.js >> atualizar.log
        exit /b 1
    )
)

REM 2. Patch: Invoices (Iugu)
echo [%date% %time%] Executando patch-invoices.js... >> atualizar.log
node patch-invoices.js >> atualizar.log 2>&1
if %errorlevel% neq 0 (
    echo [%date% %time%] AVISO: patch-invoices.js falhou >> atualizar.log
)

REM 3. Patch: Oráculo (Fabric painéis + configs)
echo [%date% %time%] Executando patch-oraculo.js... >> atualizar.log
node patch-oraculo.js >> atualizar.log 2>&1
if %errorlevel% neq 0 (
    echo [%date% %time%] AVISO: patch-oraculo.js falhou >> atualizar.log
)

REM 4. Verificar se dados.js mudou
cd /d "C:\Users\adria\AppData\Local\Temp\Ideia-vesti"
git diff --quiet CS-Sucesso-do-cliente/dados.js
if %errorlevel% equ 0 (
    echo [%date% %time%] Sem alteracoes em dados.js, nada a fazer. >> CS-Sucesso-do-cliente\atualizar.log
    exit /b 0
)

REM 5. Commit e push
echo [%date% %time%] Commitando alteracoes... >> CS-Sucesso-do-cliente\atualizar.log
git add CS-Sucesso-do-cliente/dados.js
git commit -m "Atualizacao automatica dados %date%" >> CS-Sucesso-do-cliente\atualizar.log 2>&1
git push origin main >> CS-Sucesso-do-cliente\atualizar.log 2>&1

if %errorlevel% equ 0 (
    echo [%date% %time%] Push realizado com sucesso! >> CS-Sucesso-do-cliente\atualizar.log
) else (
    echo [%date% %time%] ERRO no push >> CS-Sucesso-do-cliente\atualizar.log
)

echo [%date% %time%] Concluido. >> CS-Sucesso-do-cliente\atualizar.log
echo. >> CS-Sucesso-do-cliente\atualizar.log
