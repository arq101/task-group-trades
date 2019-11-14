# Coding task to group accepted and rejected trades
Given a bunch of trades in a XML file, the purpose is to:  
1) accept completed number of trades that fall within the limit,  
2) reject those that exceed the limit  
3) set to pending where number of trades is not completed  

Finally producing a csv output and grouping the trades by correlation ID.  


## Execute the script
Assuming a (new) virtual environment is set up already and activated.  
Designed to run with Python 3.7
```
pip3 install -r requirements.txt
```
Usage:
```
python run_analyze_trades.py input.xml
```

## Tests

Unit-tests can be run as:
```
python -m pytest -v test_run_analyze_trades.py 
```
