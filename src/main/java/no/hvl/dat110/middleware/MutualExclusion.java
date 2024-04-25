/**
 * 
 */
package no.hvl.dat110.middleware;

import java.math.BigInteger;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.LamportClock;
import no.hvl.dat110.util.Util;

/**
 * @author tdoy
 *
 */
public class MutualExclusion {
		
	private static final Logger logger = LogManager.getLogger(MutualExclusion.class);
	/** lock variables */
	private boolean CS_BUSY = false;						// indicate to be in critical section (accessing a shared resource) 
	private boolean WANTS_TO_ENTER_CS = false;				// indicate to want to enter CS
	private List<Message> queueack; 						// queue for acknowledged messages
	private List<Message> mutexqueue;						// queue for storing process that are denied permission. We really don't need this for quorum-protocol
	
	private LamportClock clock;								// lamport clock
	private Node node;
	
	public MutualExclusion(Node node) throws RemoteException {
		this.node = node;
		
		clock = new LamportClock();
		queueack = new ArrayList<Message>();
		mutexqueue = new ArrayList<Message>();
	}
	
	public synchronized void acquireLock() {
		CS_BUSY = true;
	}
	
	public void releaseLocks() {
		WANTS_TO_ENTER_CS = false;
		CS_BUSY = false;
	}

	public boolean doMutexRequest(Message message, byte[] updates) throws RemoteException {
		
		logger.info(node.nodename + " wants to access CS");
		// clear the queueack before requesting for votes
		synchronized(this) {
		queueack.clear();
		}
		// clear the mutexqueue
		mutexqueue.clear();
		// increment clock
		synchronized(clock) {
		clock.increment();
		}
		// adjust the clock on the message, by calling the setClock on the message
				message.setClock(clock.getClock());
		// wants to access resource - set the appropriate lock variable
	synchronized(this) {
		WANTS_TO_ENTER_CS = true;
	}
		
		// start MutualExclusion algorithm
		
			// first, call removeDuplicatePeersBeforeVoting. A peer can hold/contain 2 replicas of a file. This peer will appear twice
	removeDuplicatePeersBeforeVoting();
	 List<Message> activenodes = removeDuplicatePeersBeforeVoting();
     multicastMessage(message, activenodes);      // Multicast vote request

     boolean allacksreceived = areAllMessagesReturned(activenodes.size());

     if (allacksreceived) {
         acquireLock();                           // Acquire lock to enter CS
         node.broadcastUpdatetoPeers(updates);    // Assuming this method exists to broadcast updates
         mutexqueue.clear();                      // Clear the mutex queue after operations
     }

     WANTS_TO_ENTER_CS = false;                   // Done with CS

     return allacksreceived;
 }
	
	// multicast message to other processes including self
	private void multicastMessage(Message message, List<Message> activenodes) throws RemoteException {
	    logger.info("Number of peers to vote = " + activenodes.size());

	    // Iterate over the activenodes
	    for (Message metadata : activenodes) {
	        try {
	            // Obtain a stub for each node from the registry
	            NodeInterface nodeStub = Util.getProcessStub(metadata.getNodeName(), metadata.getPort());

	            // Call onMutexRequestReceived()
	            nodeStub.onMutexRequestReceived(message);
	        } catch (RemoteException e) {
	            // Handle RemoteException if necessary
	            logger.error("Error communicating with peer: " + e.getMessage());
	        }
	    }
	}
	
	public void onMutexRequestReceived(Message message) throws RemoteException {
		// Increment the local clock
	    clock.increment();

	    // If message is from self, acknowledge, and call onMutexAcknowledgementReceived()
	    if (message.getNodeName().equals(node.getNodeName())) {
	        // Acknowledge
	        node.onMutexAcknowledgementReceived(message);
	        return; // Exit method, no further processing needed
	    }

	    int caseid = -1;

	    // Write if statement to transition to the correct caseid in the doDecisionAlgorithm
	    if (!CS_BUSY && !WANTS_TO_ENTER_CS) {
	        // Receiver is not accessing shared resource and does not want to (send OK to sender)
	        caseid = 0;
	    } else if (CS_BUSY) {
	        // Receiver already has access to the resource (don't reply but queue the request)
	        caseid = 1;
	    } else if (WANTS_TO_ENTER_CS) {
	        // Receiver wants to access resource but is yet to - compare own message clock to received message's clock
	        if (message.getClock() < clock.getClock() || (message.getClock() == clock.getClock() && message.getNodeName().compareTo(node.getNodeName()) < 0)) {
	            caseid = 2;
	        } else {
	            caseid = 3;
	        }
	    }

	    // Check for decision
	    doDecisionAlgorithm(message, mutexqueue, caseid);
	}
	
	public void doDecisionAlgorithm(Message message, List<Message> queue, int condition) throws RemoteException {
		
		String procName = message.getNodeName();
		int port = message.getPort();
		
		switch(condition) {
		
			/** case 1: Receiver is not accessing shared resource and does not want to (send OK to sender) */
			case 0: {
				
				// get a stub for the sender from the registry
				 NodeInterface senderStub = Util.getProcessStub(procName, port);
				 // Acknowledge message
		            node.onMutexAcknowledgementReceived(message);

		            // Send acknowledgement back by calling onMutexAcknowledgementReceived()
		            senderStub.onMutexAcknowledgementReceived(message);

		            break;
			}
		
			/** case 2: Receiver already has access to the resource (dont reply but queue the request) */
			case 1: {
				
				// queue this message
				 queue.add(message);
				break;
			}
			
			/**
			 *  case 3: Receiver wants to access resource but is yet to (compare own message clock to received message's clock
			 *  the message with lower timestamp wins) - send OK if received is lower. Queue message if received is higher
			 */
			case 2: {
				  // Check the clock of the sending process (note that the correct clock is in the received message)
	            int senderClock = message.getClock();

	            // Own clock of the receiver (note that the correct clock is in the node's message)
	            int receiverClock = clock.getClock();

	            // Compare clocks, the lowest wins
	            if (senderClock < receiverClock || (senderClock == receiverClock && procName.compareTo(node.getNodeName()) < 0)) {
	                // If sender wins, acknowledge the message, obtain a stub and call onMutexAcknowledgementReceived()
	                node.onMutexAcknowledgementReceived(message);
	                NodeInterface senderStub = Util.getProcessStub(procName, port);
	                senderStub.onMutexAcknowledgementReceived(message);
	            } else {
	                // If sender loses, queue it
	                queue.add(message);
	            }

	            break;
			}
			
			default: break;
		}
		
	}
	
	public void onMutexAcknowledgementReceived(Message message) throws RemoteException {
		
		// add message to queueack
		queueack.add(message);
		
	}
	
	// multicast release locks message to other processes including self
	public void multicastReleaseLocks(Set<Message> activenodes) {
	    logger.info("Releasing locks from = " + activenodes.size());
	    
	    // Iterate over the activenodes
	    for (Message metadata : activenodes) {
	        try {
	            // Obtain a stub for each node from the registry
	            NodeInterface nodeStub = Util.getProcessStub(metadata.getNodeName(), metadata.getPort());
	            
	            // Call releaseLocks()
	            nodeStub.releaseLocks();
	        } catch (RemoteException e) {
	            // Handle RemoteException if necessary
	            logger.error("Error communicating with peer: " + e.getMessage());
	        }
	    }
	}
	
	private boolean areAllMessagesReturned(int numvoters) throws RemoteException {
	    logger.info(node.getNodeName() + ": size of queueack = " + queueack.size());
	    
	    // Check if the size of the queueack is the same as the numvoters
	    boolean allMessagesReturned = (queueack.size() == numvoters);
	    
	    // Clear the queueack
	    queueack.clear();
	    
	    // Return true if all messages are returned, false otherwise
	    return allMessagesReturned;
	}
	
	private List<Message> removeDuplicatePeersBeforeVoting() {
		
		List<Message> uniquepeer = new ArrayList<Message>();
		for(Message p : node.activenodesforfile) {
			boolean found = false;
			for(Message p1 : uniquepeer) {
				if(p.getNodeName().equals(p1.getNodeName())) {
					found = true;
					break;
				}
			}
			if(!found)
				uniquepeer.add(p);
		}		
		return uniquepeer;
	}
}