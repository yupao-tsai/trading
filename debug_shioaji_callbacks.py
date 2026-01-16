import shioaji as sj
import inspect

print("Attributes of sj.Shioaji:")
for name, member in inspect.getmembers(sj.Shioaji):
    if "callback" in name:
        print(f"  {name}")

