from datetime import datetime, timedelta

# Current date and time
now = datetime.now()
print("Now:", now)       # year, month, date, hour, minute, second, microsecond


year = now.strftime("%Y")
print("year:", year)

year = now.strftime("%y")         # year in short => 25
print("year in short:", year)

year = now.strftime("%m")        # month in digits
print("month in digits:", year)

year = now.strftime("%B")        # month in word
print("month in word:", year)

year = now.strftime("%b")        # month in short 
print("month in short:", year)

year = now.strftime("%d")        # date 
print("date:", year)

year = now.strftime("%D")        # dd/mm/yy  => 04/16/25
print("dd/mm/yy:", year)

year = now.strftime("%H")        # 24 hr  => 04/16/25
print("24 hr:", year)

year = now.strftime("%I")        # 12 hr  => 04/16/25
print("12 hr:", year)

year = now.strftime("%p")        # am/pm  => 04/16/25
print("am/pm:", year)

# Specific date
dt = datetime(2023, 12, 25, 10, 30)
print("Custom datetime:", dt)
print("time only", dt.time())
print("year only", dt.year)
print("month only", dt.month)
print("date only", dt.date())
print("day only", dt.day)
print("hour only", dt.hour)

# Date only
date_only = datetime.today().date()
print("Today (date only):", date_only)

# Formatting
now = datetime.now()
formatted = now.strftime("%Y-%m-%d %H:%M:%S")
print("Formatted:", formatted)

# Parsing string to datetime
parsed = datetime.strptime("2025-04-10", "%Y-%m-%d")
print("Parsed:", parsed)

parsed = datetime.strptime("2025-04-10 10:45:40", "%Y-%m-%d %H:%M:%S")
print("Parsed:", parsed)


today = datetime.today()
print("Today:", today)              # Current local date and time

print("using now:", datetime.now())   # Current local date and time by default, can accept a timezone

print("Yesterday:", today - timedelta(days=1))
print("In 7 days:", today + timedelta(days=7))

yest1 = timedelta(hours=10)
print("10 hours back from today", today-yest1)


date_str = "2025-04-10"
converted_date = datetime.strptime(date_str, "%Y-%m-%d")

print("Converted datetime:", converted_date)