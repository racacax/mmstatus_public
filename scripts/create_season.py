import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from datetime import datetime

from models import Season

start_date = input("Start date (YYYY-MM-DD HH:mm:ss):")
end_date = input("End date (YYYY-MM-DD HH:mm:ss):")
season_name = input("Season name:")
start_date_f = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
end_date_f = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")

print(Season.create(name=season_name, start_time=start_date_f, end_time=end_date_f))
print("Season created")
