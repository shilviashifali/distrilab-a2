package common;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Set;

public interface WorkerService extends Remote {
    int getId() throws RemoteException;
    boolean ping() throws RemoteException;
    void addNeighbour(int neighbourId, WorkerService neighbour) throws RemoteException;
    void removeNeighbour(int neighbourId) throws RemoteException;
    Set<Integer> getNeighbourIds() throws RemoteException;

    // Person 2: election / coordinator methods go here
    int getJac() throws RemoteException;
    int getCoordinatorId() throws RemoteException;
    ElectionCandidate receiveElection(String electionId, int senderId) throws RemoteException;
    void receiveCoordinator(String electionId, int coordinatorId) throws RemoteException;
    void startElection() throws RemoteException;

    // Person 3: job execution methods
    <R extends Serializable> R executeTask(Job<R> job) throws RemoteException;
    JobResult submitJob(Job<?> job) throws RemoteException;
}