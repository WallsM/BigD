
def coolStory(keyValue):
    result = ""
    count = -1
    for item in keyValue[1]:
        count = count + 1
        if len(item) == 1:
            result = keyValue[1].pop(count)[0]
    return ((keyValue[0], result), keyValue[1])


def filter(keyValue):
    
    # drop item if it does not contain WalletID
    for item in keyValue[1]:
        if len(item) == 1:
            return True
    return False
