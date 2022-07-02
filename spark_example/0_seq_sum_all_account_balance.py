from os import listdir

path = "/Users/sugi/learning/bigdata/data"  # change this path to yours
files = listdir(path)
print(files)

sum_all_account_balance = 0
for file in files:
    with open(f"/Users/sugi/learning/bigdata/data/{file}") as f:
        for idx, line in enumerate(f.readlines()):
            if idx == 0:
                continue
            sum_all_account_balance += int(line.split(",")[1])

with open("0_seq_sum_all_account_balance_result.txt", "w") as f:
    f.write(str(sum_all_account_balance))
