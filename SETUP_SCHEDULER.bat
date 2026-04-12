schtasks /create /tn "SmartMarket_Daily" ^
  /tr "C:\smartmarket\smartmarket_api\SmartMarket\integrator\.venv\Scripts\python.exe C:\smartmarket\smartmarket_api\SmartMarket\integrator\MASTER_RUN.py" ^
  /sc daily /st 02:00 /f

echo SmartMarket daily scrape scheduled for 02:00

schtasks /create /tn "SmartMarket_6H" ^
  /tr "C:\smartmarket\smartmarket_api\SmartMarket\integrator\.venv\Scripts\python.exe C:\smartmarket\smartmarket_api\SmartMarket\integrator\MASTER_RUN.py --quick" ^
  /sc hourly /mo 6 /f

echo SmartMarket quick scrape scheduled every 6 hours (SmartMarket_6H)
