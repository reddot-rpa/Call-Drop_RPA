from cryptography.fernet import Fernet
import os


class Encryption:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    secret_file_dir = current_dir[:-5] + "apps/secret.txt"

    def generate_key(self):
        key = Fernet.generate_key()
        with open(self.secret_file_dir, 'wb') as f:
            f.write(key)

    def get_key(self):
        with open(self.secret_file_dir, 'rb') as f:
            key = f.readline()
        return key

    def encrypt(self, plaintext, key):
        cipher_suit = Fernet(key)
        text = plaintext.encode('UTF-8')
        cipher_text = cipher_suit.encrypt(text)
        return cipher_text

    def decrypt(self, ciphertext, key):
        cipher_suit = Fernet(key)
        plain_text = cipher_suit.decrypt(ciphertext)
        return plain_text.decode('UTF-8')