package common;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;

public interface BootstrapService extends Remote {
    int register(WorkerService worker) throws RemoteException;
    void unregister(int workerId) throws RemoteException;
    WorkerInfo getRandomActiveWorker(int excludeId) throws RemoteException;
    Map<Integer, WorkerService> getActiveWorkers() throws RemoteException;
    void setCoordinator(int workerId) throws RemoteException;
    WorkerInfo getCoordinator() throws RemoteException;
}