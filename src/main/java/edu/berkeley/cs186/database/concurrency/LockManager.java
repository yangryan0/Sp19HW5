package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.BaseTransaction;
import edu.berkeley.cs186.database.Database;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have
 * what locks on what resources. The lock manager should generally **not**
 * be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with
 * multiple levels of granularity (you can and should treat ResourceName
 * as a generic Object, rather than as an object encapsulating levels of
 * granularity, in this class).
 *
 * It follows that LockManager should allow **all**
 * requests that are valid from the perspective of treating every resource
 * as independent objects, even if they would be invalid from a
 * multigranularity locking perspective. For example, if LockManager#acquire
 * is called asking for an X lock on Table A, and the transaction has no
 * locks at the time, the request is considered valid (because the only problem
 * with such a request would be that the transaction does not have the appropriate
 * intent locks, but that is a multigranularity concern).
 *
 * Each resource the lock manager manages has its own queue of LockRequest objects
 * representing a request to acquire (or promote/acquire-and-release) a lock that
 * could not be satisfied at the time. This queue should be processed every time
 * a lock on that resource gets released, starting from the first request, and going
 * in order until a request cannot be satisfied. Requests taken off the queue should
 * be treated as if that transaction had made the request right after the resource was
 * released in absence of a queue (i.e. removing a request by T1 to acquire X(db) should
 * be treated as if T1 had just requested X(db) and there were no queue on db: T1 should
 * be given the X lock on db, and put in an unblocked state via BaseTransaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();
    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // You may add helper methods here if you wish
    }

    // You should not modify or use this directly.
    protected Map<Object, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to NAME.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    // You may add helper methods here if you wish

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION, and releases all locks
     * in RELEASELOCKS after acquiring the lock, in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * Locks in RELEASELOCKS should be released only after the requested lock has been acquired.
     * The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on NAME **should not** change the
     * acquisition time of the lock on NAME, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), acquire X(A) and release S(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by TRANSACTION and
     * isn't being released
     * @throws NoLockHeldException if no lock on a name in RELEASELOCKS is held by TRANSACTION
     */
    public void acquireAndRelease(BaseTransaction transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks)
    throws DuplicateLockRequestException, NoLockHeldException {
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        boolean block = false;
        synchronized (this) {
            List<Lock> locksHeld = getLocks(transaction);
            List<Lock> realReleaseLocks = new ArrayList<>();
            for (Lock lock : locksHeld) {
                for (ResourceName resourceName : releaseLocks) {
                    if (lock.name.equals(resourceName)) {
                        realReleaseLocks.add(lock);
                        break;
                    }
                }
                if (lock.name.equals(name) && !realReleaseLocks.contains(lock)) {
                    throw new DuplicateLockRequestException("Duplicate lock");
                }
            }
            if (realReleaseLocks.size() != releaseLocks.size()) {
                throw new NoLockHeldException("No lock held");
            }
            ResourceEntry resource = getResourceEntry(name);
            Lock newLock = new Lock(name, lockType, transaction.getTransNum());
            LockRequest acquireRequest = new LockRequest(transaction, newLock, realReleaseLocks);
            resource.waitingQueue.addFirst(acquireRequest);
            int qLen = resource.waitingQueue.size();
            processQueue(name);
            if (resource.waitingQueue.size() == qLen) {
                block = true;
            }
        }
        if (block) {
            transaction.block();
        }
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by
     * TRANSACTION
     */
    public void acquire(BaseTransaction transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        Lock lockEntry = new Lock(name, lockType, transaction.getTransNum());
        ResourceEntry r_entry = getResourceEntry(name);
        if (transactionLocks.containsKey(transaction.getTransNum())) {
            for (Lock l: transactionLocks.get(transaction.getTransNum())) {
                if (l.name == name && l.lockType == lockType) {
                    throw new DuplicateLockRequestException("Duplicate Lock");
                }
            }
        }
        if (r_entry.waitingQueue.size() != 0) {
            r_entry.waitingQueue.addLast(new LockRequest(transaction, lockEntry));
            transaction.block();
            return;
        }
        for (Lock l:r_entry.locks) {
            if (!LockType.compatible(l.lockType, lockType)){
                r_entry.waitingQueue.addLast(new LockRequest(transaction, lockEntry));
                transaction.block();
                return;
            }
        }
        synchronized (this) {
            if (transactionLocks.containsKey(transaction.getTransNum())) {
                List<Lock> currList = transactionLocks.get(transaction.getTransNum());
                currList.add(lockEntry);
                transactionLocks.put(transaction.getTransNum(), currList);
            } else {
                List<Lock> newEntry = new ArrayList<>();
                newEntry.add(lockEntry);
                transactionLocks.put(transaction.getTransNum(), newEntry);
            }
            getResourceEntry(name).locks.add(lockEntry);
        }
    }

    private boolean containsName(Lock l, LockRequest LR) {
        for (Lock lock : LR.releasedLocks) {
            if (lock.name.equals(l.name)) {
                return true;
            }
        }
        return false;
    }

    private void processQueue(ResourceName name) {
        ResourceEntry rEntry = getResourceEntry(name);
        while (rEntry.waitingQueue.size() > 0) {
            LockRequest currLR = rEntry.waitingQueue.peek();
            for (Lock l : currLR.releasedLocks) {
                removeLock(currLR.transaction, l.name);
            }
            if (conflicts(currLR.lock, name)) {
                for (Lock l : currLR.releasedLocks) {
                    giveLock(currLR.transaction, l.name, l);
                }
                break;
            }
            rEntry.waitingQueue.pop();
            giveLock(currLR.transaction, name, currLR.lock);
            currLR.transaction.unblock();
        }
    }

    private void giveLock(BaseTransaction transaction, ResourceName name, Lock lockEntry) {
        if (transactionLocks.containsKey(transaction.getTransNum())) {
            List<Lock> currList = transactionLocks.get(transaction.getTransNum());
            currList.add(lockEntry);
            transactionLocks.put(transaction.getTransNum(), currList);
        } else {
            List<Lock> newEntry = new ArrayList<>();
            newEntry.add(lockEntry);
            transactionLocks.put(transaction.getTransNum(), newEntry);
        }
        getResourceEntry(name).locks.add(lockEntry);
    }

    private void removeLock(BaseTransaction transaction, ResourceName name) {
        List<Lock> tlEntry = transactionLocks.get(transaction.getTransNum());
        ResourceEntry rEntry = getResourceEntry(name);
        int removal_index = 0;
        for (int i = 0; i < tlEntry.size(); i++) {
            if (tlEntry.get(i).name == name) {
                removal_index = i;
            }
        }
        tlEntry.remove(removal_index);
        for (int i = 0; i < rEntry.locks.size(); i++) {
            if (rEntry.locks.get(i).transactionNum == transaction.getTransNum()) {
                removal_index = i;
            }
        }
        rEntry.locks.remove(removal_index);
    }

    private boolean conflicts(Lock lock, ResourceName name) {
        for (Lock l : getResourceEntry(name).locks) {
            if (!LockType.compatible(lock.lockType, l.lockType)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Error checking must be done before the lock is released.
     *
     * NAME's queue should be processed after this call. If any requests in
     * the queue have locks to be released, those should be released, and the
     * corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     */
    public void release(BaseTransaction transaction, ResourceName name)
    throws NoLockHeldException {
        // You may modify any part of this method.
        List<Lock> tlEntry = transactionLocks.get(transaction.getTransNum());
        if (tlEntry == null) {
            throw new NoLockHeldException("No Lock Found");
        }
        boolean found = false;
        for (Lock l : tlEntry) {
            if (l.name == name) {
                found = true;
            }
        }
        if (!found) {
            throw new NoLockHeldException("No Lock Found");
        }
        synchronized (this) {
            removeLock(transaction, name);
            processQueue(name);
        }
    }

    /**
     * Promote TRANSACTION's lock on NAME to NEWLOCKTYPE (i.e. change TRANSACTION's lock
     * on NAME from the current lock type to NEWLOCKTYPE, which must be strictly more
     * permissive).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * A lock promotion **should not** change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a
     * NEWLOCKTYPE lock on NAME
     * @throws NoLockHeldException if TRANSACTION has no lock on NAME
     * @throws InvalidLockException if the requested lock type is not a promotion. A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     */
    public void promote(BaseTransaction transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // You may modify any part of this method.
        List<Lock> currLocks = getLocks(transaction);
        boolean found = false;
        Lock currLock = null;
        for (Lock l : currLocks) {
            if (l.name.equals(name)) {
                currLock = l;
                found = true;
                if (l.lockType == newLockType) {
                    throw new DuplicateLockRequestException("Duplicate Locks");
                }
                if (!LockType.substitutable(newLockType, l.lockType) || newLockType.equals(l.lockType)) {
                    throw new InvalidLockException("Invalid promotion");
                }
            }
        }
        if (!found) {
            throw new NoLockHeldException("No Lock Found");
        }
        synchronized (this) {
            Lock newLock = new Lock(name, newLockType, transaction.getTransNum());
            List<Lock> releasedLocks = new ArrayList<>();
            releasedLocks.add(currLock);
            LockRequest newLR = new LockRequest(transaction, newLock, releasedLocks);
            getResourceEntry(name).waitingQueue.addFirst(newLR);
            processQueue(name);
        }
    }

    /**
     * Return the type of lock TRANSACTION has on NAME (return NL if no lock is held).
     */
    public synchronized LockType getLockType(BaseTransaction transaction, ResourceName name) {
        List<Lock> l = transactionLocks.get(transaction.getTransNum());
        if (l == null) {
            return LockType.NL;
        }
        for (Lock a : l) {
            if (a.name == name) {
                return a.lockType;
            }
        }
        return LockType.NL;
    }

    /**
     * Returns the list of locks held on NAME, in order of acquisition.
     * A promotion or acquire-and-release should count as acquired
     * at the original time.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks locks held by
     * TRANSACTION, in order of acquisition. A promotion or
     * acquire-and-release should count as acquired at the original time.
     */
    public synchronized List<Lock> getLocks(BaseTransaction transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                               Collections.emptyList()));
    }

    /**
     * Create a lock context for the database. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        contexts.putIfAbsent("database", new LockContext(this, null, "database"));
        return contexts.get("database");
    }

    /**
     * Create a lock context with no parent. Cannot be called "database".
     */
    public synchronized LockContext orphanContext(Object name) {
        if (name.equals("database")) {
            throw new IllegalArgumentException("cannot create orphan context named 'database'");
        }
        contexts.putIfAbsent(name, new LockContext(this, null, name));
        return contexts.get(name);
    }
}
