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
		
		queueack.clear();
		mutexqueue.clear();
		clock.increment();
		
		message.setClock(clock.getClock());
				
		WANTS_TO_ENTER_CS = true;
		
		// start MutualExclusion algorithm
		List<Message> activernodes = removeDuplicatePeersBeforeVoting();

		multicastMessage(message, activernodes);

		int repliedCount = (int) activernodes.stream().filter(m -> m.isAcknowledged()).count(); // maybe wrong
		System.out.println(repliedCount);
		System.out.println(activernodes.size());
		
		boolean permission = areAllMessagesReturned(activernodes.size());
		
		if (permission) {
			acquireLock();
			node.broadcastUpdatetoPeers(updates);
			mutexqueue.clear();
		}
		
		return permission;
	}
	
	// multicast message to other processes including self
	private void multicastMessage(Message message, List<Message> activenodes) throws RemoteException {
		
		logger.info("Number of peers to vote = "+activenodes.size());
		
		for (Message m : activenodes) {
			Util.getProcessStub(m.getNodeName(), m.getPort()).onMutexRequestReceived(message);
		}
	}
	
	public void onMutexRequestReceived(Message message) throws RemoteException {
		clock.increment();
		
		if (message.getNodeID().equals(node.getNodeID())) {
			message.setAcknowledged(true);
			
			onMutexAcknowledgementReceived(message);
		}
		
		int caseid = -1;

		if (!CS_BUSY && !WANTS_TO_ENTER_CS) {
			caseid = 0;
		} else if (CS_BUSY) {
			caseid = 1;
		} else if (WANTS_TO_ENTER_CS) {
			caseid = 2;
		}

		// check for decision
		doDecisionAlgorithm(message, mutexqueue, caseid);
	}
	
	public void doDecisionAlgorithm(Message message, List<Message> queue, int condition) throws RemoteException {
		
		String procName = message.getNodeName();
		int port = message.getPort();
		
		switch(condition) {
		
			/** case 1: Receiver is not accessing shared resource and does not want to (send OK to sender) */
			case 0: {
				NodeInterface stub = Util.getProcessStub(procName, port);
			
				message.setAcknowledged(true);
				
				stub.onMutexAcknowledgementReceived(message);
				
				break;
			}
		
			/** case 2: Receiver already has access to the resource (dont reply but queue the request) */
			case 1: {
				queue.add(message);
				
				break;
			}
			
			/**
			 *  case 3: Receiver wants to access resource but is yet to (compare own message clock to received message's clock
			 *  the message with lower timestamp wins) - send OK if received is lower. Queue message if received is higher
			 */
			case 2: {
				int senderClock = message.getClock();
				int ownClock = node.getMessage().getClock();
				
				if (senderClock < ownClock) {
					message.setAcknowledged(true);
					
					NodeInterface stub = Util.getProcessStub(procName, port);
			
					stub.onMutexAcknowledgementReceived(message);
				} else if (senderClock == ownClock && message.getNodeID().compareTo(node.getNodeID()) < 0) {
					message.setAcknowledged(true);
					
					NodeInterface stub = Util.getProcessStub(procName, port);
					
					stub.onMutexAcknowledgementReceived(message);
				} else {
					queue.add(message);
				}

				break;
			}
			
			default: break;
		}
		
	}
	
	public void onMutexAcknowledgementReceived(Message message) throws RemoteException {
		queueack.add(message);
	}
	
	// multicast release locks message to other processes including self
	public void multicastReleaseLocks(Set<Message> activenodes) throws RemoteException { // added throw
		logger.info("Releasing locks from = "+activenodes.size());
		
		for (Message m : activenodes) {
			Util.getProcessStub(m.getNodeName(), m.getPort()).releaseLocks();
		}
	}
	
	private boolean areAllMessagesReturned(int numvoters) throws RemoteException {
		logger.info(node.getNodeName()+": size of queueack = "+queueack.size());
		
		int size = queueack.size();
		
		queueack.clear();
		
		return size == numvoters;
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