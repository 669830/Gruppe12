package no.hvl.dat110.util;

/**
 * exercise/demo purpose in dat110
 * @author tdoy
 *
 */

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hash { 
	
	
public static BigInteger hashOf(String entity) {	
		
		BigInteger hashint = null;
		
		// Task: Hash a given string using MD5 and return the result as a BigInteger.
		
		// we use MD5 with 128 bits digest
		
		// compute the hash of the input 'entity'
		
		// convert the hash into hex format
		
		// convert the hex into BigInteger
		
		// return the BigInteger
		
	try {
		MessageDigest md = MessageDigest.getInstance("MD5");
		
		md.update(entity.getBytes());
		
		byte[] digest = md.digest();
		
		// Convert the hash into hex format
        String hexHash = toHex(digest);

        // Convert the hex into BigInteger
        hashint = new BigInteger(hexHash, 16);

    } catch (NoSuchAlgorithmException  e) {
        // Handle exceptions
        e.printStackTrace();
    }
		
		return hashint;
	}
	
	public static BigInteger addressSize() {
		
		// Task: compute the address size of MD5
		
		// compute the number of bits = bitSize()
		
		// compute the address size = 2 ^ number of bits
		
		// return the address size
		BigInteger addressSize = null;
		

	    try {
	    	int numberOfBits = bitSize();
	    	
	    	addressSize = BigInteger.valueOf(2).pow(numberOfBits);
	    }catch(NoSuchAlgorithmException e) {
	    	e.printStackTrace();
	    }

	    return addressSize;
	}
	
	public static int bitSize()  throws NoSuchAlgorithmException{
		
		MessageDigest md = MessageDigest.getInstance("MD5");
		
		int digestlen = md.getDigestLength();
		
		// find the digest length
		
		return digestlen*8;
	}
	
	public static void main(String[] args) {
		BigInteger result = addressSize();
		System.out.print("Address size of MD5 hash" + result);
	}
	
	public static String toHex(byte[] digest) {
		StringBuilder strbuilder = new StringBuilder();
		for(byte b : digest) {
			strbuilder.append(String.format("%02x", b&0xff));
		}
		return strbuilder.toString();
	}

}