import random


# noinspection PyTypeChecker
class CryptoPassPhase:
    chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ' + \
            'abcdefghijklmnopqrstuvwxyz' + \
            '0123456789' + \
            ':.;,?!@#$%&()+=-*/_<> []{}`~^"\'\\'

    def __init__(self):
        return self

    @staticmethod
    def generate_key():
        shuffled = sorted(CryptoPassPhase.chars, key=lambda k: random.random())
        return dict(zip(CryptoPassPhase.chars, shuffled))

    @staticmethod
    def encrypt(key, plaintext):
        return ''.join(key[l] for l in plaintext)

    @staticmethod
    def decrypt(key, ciphertext):
        flipped = {v: k for k, v in key.items()}
        return ''.join(flipped[l] for l in ciphertext)

# ============= Sample Class Kit Start ========================
# key = CryptoPassPhase.generate_key()
# encrypted = CryptoPassPhase.encrypt(Key, PASSWORD)
# encrypted = CryptoPassPhase.encrypt(key, 'Hi There, How are You.')
# decrypted = CryptoPassPhase.decrypt(Key, encrypted)
# print('Key=', key)
# print('encrypted=', encrypted)
# print('decrypted=', decrypted)
# ============= Sample Class Kit End ========================
