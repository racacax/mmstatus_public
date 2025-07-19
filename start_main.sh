pkill -f manager.py
pkill -f server.py
python manager.py &
python server.py
echo "App crashed. Restarting..."
$0