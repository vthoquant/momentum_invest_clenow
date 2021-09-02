@echo off
call activate base
cd "C:\Users\vivin\Spyder projects\momentum_invest_clenow"
echo =======================================================
echo running daily clenow-style portfolio analysis
echo =======================================================
python runner.py
conda deactivate
