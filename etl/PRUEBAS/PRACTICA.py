

def is_sorted(lst):
    asc, desc = True, True
    for i in range(len(lst) - 1):
        if lst[i] > lst[i + 1]:
            asc = False
    for i in range(len(lst) - 1):
        if lst[i] < lst[i + 1]:
            desc = False
    return asc or desc


print(is_sorted([1,2,3,4]))
