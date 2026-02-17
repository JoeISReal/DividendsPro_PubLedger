import base64
import os
import nacl.signing
import nacl.encoding
from typing import Tuple

class Signer:
    def __init__(self, private_key_hex: str = None):
        """
        Initialize the signer with a private key.
        
        Args:
            private_key_hex (str): The private key in hex format.
                                   If None, attempts to load from LEDGER_PRIVATE_KEY env var.
        """
        if private_key_hex is None:
            private_key_hex = os.environ.get("LEDGER_PRIVATE_KEY")
            
        if not private_key_hex:
            raise ValueError("Private key not provided and LEDGER_PRIVATE_KEY not set")
            
        try:
            # Decode the hex string into bytes
            self.signing_key = nacl.signing.SigningKey(private_key_hex, encoder=nacl.encoding.HexEncoder)
        except Exception as e:
            raise ValueError(f"Invalid private key format: {e}")

    def sign_entry(self, entry_hash: str) -> str:
        """
        Sign the entry hash using Ed25519.
        
        Args:
            entry_hash (str): The hex string of the entry hash.
            
        Returns:
            str: The signature encoded in Base64.
        """
        if not entry_hash:
            raise ValueError("Entry hash cannot be empty")
            
        # Sign the hash (bytes)
        signed = self.signing_key.sign(entry_hash.encode('utf-8'))
        
        # Return signature part only, base64 encoded
        return base64.b64encode(signed.signature).decode('utf-8')

    def get_public_key(self) -> str:
        """
        Returns the public key in hex format.
        """
        return self.signing_key.verify_key.encode(encoder=nacl.encoding.HexEncoder).decode('utf-8')

def generate_keypair() -> Tuple[str, str]:
    """
    Generates a new Ed25519 keypair.
    
    Returns:
        Tuple[str, str]: (private_key_hex, public_key_hex)
    """
    signing_key = nacl.signing.SigningKey.generate()
    verify_key = signing_key.verify_key
    
    private_hex = signing_key.encode(encoder=nacl.encoding.HexEncoder).decode('utf-8')
    public_hex = verify_key.encode(encoder=nacl.encoding.HexEncoder).decode('utf-8')
    
    return private_hex, public_hex
