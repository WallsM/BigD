import functions as f
from pyspark import SparkContext
sc = SparkContext()
vin = sc.textFile("vin.csv")
vout = sc.textFile("vout.csv")
# WANT TO FIND SENDER IN TRANSACTION (FROM) to TRANSACTION (TO)

 # key(line 7) is (tx_hash (transaction from), vout (id of out in previous transation))
 # value(line 8) is txid (transaction to)
vin = vin.map(lambda line: line.split(",")).filter(lambda list: len(list) == 3)
vin = vin.map(lambda list: ((list[1].strip(" "), list[2].strip(" ")), [list[0].strip(" ")]))


# key(line 12) is (hash (same as tx_hash), n (same as vout))
# value(line 13) is (value (amount of BTC), publicKey (walletID for sender of hash(n)))
vout = vout.map(lambda line: line.split(",")).filter(lambda list: len(list) == 4)
vout = vout.map(lambda list: ((list[0].strip(" "), list[2].strip(" ")), [list[1].strip(" "), list[3].strip(" ")]))

# combine vin and vout to have
# key: (transactionID) (from)
# value: (n, txid (to), value, receiver)
result = vin.union(vout).reduceByKey(lambda a, b: a + b)
result = result.map(lambda (key, value): ((key[0]), [key[1]] + value))
# filtering nodes that don't have edges??? (like a vin with no vout or a vout with no vin)
result = result.filter(lambda (key, value): len(value) == 4)

# wallets contains
# key: transactionID
# value: [walletID] - list because of me for loop later
wallets = result.map(lambda (key, value): (value[1], [value[3]]))
wallets = wallets.reduceByKey(lambda x, y: x + y)
# making sure wallets has only one value
wallets = wallets.map(lambda (key, value): (key, [value[0]]))

# key: (transactionID) (from)
# value: [(n, txid(to), value, reciever)] this is a list of the 4-tuple for each out (n)
result2 = result.reduceByKey(lambda x, y: [x] + [y])



# key is transactionID
# Value is list of 4-tuple and [WalletID]
result3 = result2.union(wallets)
result3 = result3.reduceByKey(lambda x, y: [x] + [y])
# drop item if there are no transactions for the walletID
result3 = result3.filter(lambda (key, value): len(value) != 1)
result3 = result3.filter(lambda x: f.filter(x))

# does not work right now. Trying to remove walletID from value and put it in key
result4 = result3.map(lambda x: f.coolStory(x))
result4.saveAsTextFile("pleaseHelp")

# [res2.pop()[0] for thing in res2 if len(thing) == 1][0] - ignore
