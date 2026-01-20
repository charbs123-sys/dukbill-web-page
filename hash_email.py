import hashlib

email = "george@dukbill.com"

print(hashlib.sha256(email.encode("utf-8")).hexdigest())