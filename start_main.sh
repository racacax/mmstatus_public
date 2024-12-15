pkill -f python
python manager.py &
python server.py
echo "App crashed. Restarting..."
$0