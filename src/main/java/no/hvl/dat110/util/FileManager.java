package no.hvl.dat110.util;


/**
 * @author tdoy
 * dat110 - project 3
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.rmi.RemoteException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import no.hvl.dat110.middleware.Message;
import no.hvl.dat110.rpc.interfaces.NodeInterface;

public class FileManager {
	
private static final Logger logger = LogManager.getLogger(FileManager.class);
	
	private BigInteger[] replicafiles;							// array stores replicated files for distribution to matching nodes
	private int numReplicas;									// let's assume each node manages nfiles (5 for now) - can be changed from the constructor
	private NodeInterface chordnode;
	private String filepath; 									// absolute filepath
	private String filename;									// only filename without path and extension
	private BigInteger hash;
	private byte[] bytesOfFile;
	private String sizeOfByte;
	
	private Set<Message> activeNodesforFile = null;
	
	public FileManager(NodeInterface chordnode) throws RemoteException {
		this.chordnode = chordnode;
	}
	
	public FileManager(NodeInterface chordnode, int N) throws RemoteException {
		this.numReplicas = N;
		replicafiles = new BigInteger[N];
		this.chordnode = chordnode;
	}
	
	public FileManager(NodeInterface chordnode, String filepath, int N) throws RemoteException {
		this.filepath = filepath;
		this.numReplicas = N;
		replicafiles = new BigInteger[N];
		this.chordnode = chordnode;
	}
	
	public void createReplicaFiles() {
        for (int index = 0; index < Util.numReplicas; index++) {
            String filenameWithIndex = filename + index;
            BigInteger hashValue = Hash.hashOf(filenameWithIndex);
            replicafiles[index] = hashValue;
        }
    }
	// metoden for å hash en fil
	private byte[] hashFile(String fileName) throws NoSuchAlgorithmException{
		MessageDigest digest = MessageDigest.getInstance("SHA-256");
		digest.reset();
		digest.update(fileName.getBytes());
		return digest.digest();
	}
	
	
    /**
     * 
     * @param bytesOfFile
     * @throws IOException 
     * @throws NoSuchAlgorithmException 
     */
	
	public int distributeReplicastoPeers() throws RemoteException {

        // Randomly appoint the primary server to one of the file replicas
        Random rnd = new Random();
        int primaryIndex = rnd.nextInt(numReplicas);

        int counter = 0;

        // Create replicas of the filename
        createReplicaFiles();

        // Iterate over the replicas
        for (BigInteger replica : replicafiles) {
            // Find the successor (peer/node) for this replica
            NodeInterface successor = chordnode.findSuccessor(replica);

            // Add the replica to the successor's key ring
            successor.addKey(replica);

            // Determine if this peer is the primary server for the file
            boolean isPrimary = (counter == primaryIndex);

            // Save the file content on the successor, and mark it as primary if applicable
            successor.saveFileContent(filename, replica, bytesOfFile, isPrimary);

            // Increment counter
            counter++;
        }

        return counter;
    }
	/**
	 * 
	 * @param filename
	 * @return list of active nodes having the replicas of this file
	 * @throws RemoteException 
	 */
	public Set<Message> requestActiveNodesForFile(String filename) throws RemoteException {
		
		this.filename = filename;
		activeNodesforFile = new HashSet<Message>();
	    // Initialize the set to store active nodes for the file
		
	        // Generate the N replicas from the filename
	        createReplicaFiles();

	        // Iterate over the replicas of the file
	        for (BigInteger replica : replicafiles) {
	            // Find the successor (peer/node) for the replica
	            NodeInterface successor = chordnode.findSuccessor(replica);

	            // Get the metadata of the successor node
	            Message metadata = successor.getFilesMetadata(replica);

	            // Save the metadata in the set activeNodesforFile
	            activeNodesforFile.add(metadata);
	        }
	   

	    return activeNodesforFile;
	    
	}
	
	
	
	/**
	 * Find the primary server - Remote-Write Protocol
	 * @return 
	 */
	public NodeInterface findPrimaryOfItem() {

        // Iterate over the activeNodesforFile
       for (Message metadata : activeNodesforFile) {
           // For each active peer (saved as Message), check if it is the primary or not
           if (metadata.isPrimaryServer()) {
               // If it is the primary, return the corresponding NodeInterface
               return Util.getProcessStub(metadata.getNodeName(), metadata.getPort());
           }
       }
       // If no primary is found, return null
       return null;
   }

    /**
     * Read the content of a file and return the bytes
     * @throws IOException 
     * @throws NoSuchAlgorithmException 
     */
    public void readFile() throws IOException, NoSuchAlgorithmException {
    	
    	File f = new File(filepath);
    	
    	byte[] bytesOfFile = new byte[(int) f.length()];
    	
		FileInputStream fis = new FileInputStream(f);
        
        fis.read(bytesOfFile);
		fis.close();
		
		//set the values
		filename = f.getName().replace(".txt", "");		
		hash = Hash.hashOf(filename);
		this.bytesOfFile = bytesOfFile;
		double size = (double) bytesOfFile.length/1000;
		NumberFormat nf = new DecimalFormat();
		nf.setMaximumFractionDigits(3);
		sizeOfByte = nf.format(size);
		
		logger.info("filename="+filename+" size="+sizeOfByte);
    	
    }
    
    public void printActivePeers() {
    	
    	activeNodesforFile.forEach(m -> {
    		String peer = m.getNodeName();
    		String id = m.getNodeID().toString();
    		String name = m.getNameOfFile();
    		String hash = m.getHashOfFile().toString();
    		int size = m.getBytesOfFile().length;
    		
    		logger.info(peer+": ID = "+id+" | filename = "+name+" | HashOfFile = "+hash+" | size ="+size);
    		
    	});
    }

	/**
	 * @return the numReplicas
	 */
	public int getNumReplicas() {
		return numReplicas;
	}
	
	/**
	 * @return the filename
	 */
	public String getFilename() {
		return filename;
	}
	/**
	 * @param filename the filename to set
	 */
	public void setFilename(String filename) {
		this.filename = filename;
	}
	/**
	 * @return the hash
	 */
	public BigInteger getHash() {
		return hash;
	}
	/**
	 * @param hash the hash to set
	 */
	public void setHash(BigInteger hash) {
		this.hash = hash;
	}
	/**
	 * @return the bytesOfFile
	 */ 
	public byte[] getBytesOfFile() {
		return bytesOfFile;
	}
	/**
	 * @param bytesOfFile the bytesOfFile to set
	 */
	public void setBytesOfFile(byte[] bytesOfFile) {
		this.bytesOfFile = bytesOfFile;
	}
	/**
	 * @return the size
	 */
	public String getSizeOfByte() {
		return sizeOfByte;
	}
	/**
	 * @param size the size to set
	 */
	public void setSizeOfByte(String sizeOfByte) {
		this.sizeOfByte = sizeOfByte;
	}

	/**
	 * @return the chordnode
	 */
	public NodeInterface getChordnode() {
		return chordnode;
	}

	/**
	 * @return the activeNodesforFile
	 */
	public Set<Message> getActiveNodesforFile() {
		return activeNodesforFile;
	}

	/**
	 * @return the replicafiles
	 */
	public BigInteger[] getReplicafiles() {
		return replicafiles;
	}

	/**
	 * @param filepath the filepath to set
	 */
	public void setFilepath(String filepath) {
		this.filepath = filepath;
	}
}