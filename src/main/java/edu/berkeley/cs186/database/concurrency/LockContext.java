package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.BaseTransaction;
import org.omg.CORBA.DynAnyPackage.Invalid;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional fields/methods as you see fit.
    // The underlying lock manager.
    protected LockManager lockman;
    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected LockContext parent;
    // The name of the resource this LockContext represents.
    protected ResourceName name;
    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;
    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected Map<Long, Integer> numChildLocks;
    // The number of children that this LockContext has, if it differs from the number of times
    // LockContext#childContext was called with unique parameters: for a table, we do not
    // explicitly create a LockContext for every page (we create them as needed), but
    // the capacity should be the number of pages in the table, so we use this
    // field to override the return value for capacity().
    protected int capacity;

    // You should not modify or use this directly.
    protected Map<Object, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, Object name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, Object name, boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.capacity = 0;
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to NAME from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<Object> names = name.getNames().iterator();
        LockContext ctx;
        Object n1 = names.next();
        if (n1.equals("database")) {
            ctx = lockman.databaseContext();
        } else {
            ctx = lockman.orphanContext(n1);
        }
        while (names.hasNext()) {
            ctx = ctx.childContext(names.next());
        }
        return ctx;
    }

    private boolean parentSubstitutable(BaseTransaction transaction, LockType lockType) {
        LockType leastPermissive = LockType.parentLock(lockType);
        if (parent == null) {
            return true;
        }
        for (Lock l : lockman.getLocks(parent.name)) {
            if (LockType.substitutable(l.lockType, leastPermissive) && l.transactionNum == transaction.getTransNum()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a LOCKTYPE lock, for transaction TRANSACTION.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by TRANSACTION
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(BaseTransaction transaction, LockType lockType)
    throws InvalidLockException, DuplicateLockRequestException {
        if (readonly) {
            throw new UnsupportedOperationException("Context is read only");
        }
        if (!parentSubstitutable(transaction, lockType)) {
            throw new InvalidLockException("No parent lock");
        }
        lockman.acquire(transaction, name, lockType);
        LockContext curr = parent;
        while (curr != null) {
            if (curr.numChildLocks.containsKey(transaction.getTransNum())) {
                curr.numChildLocks.put(transaction.getTransNum(), curr.numChildLocks.get(transaction.getTransNum()) + 1);
            } else {
                curr.numChildLocks.put(transaction.getTransNum(), 1);
            }
            curr = curr.parent;
        }
    }

    public boolean checkChild(BaseTransaction transaction, Lock child) {
        List<Lock> parentLocks = new ArrayList<>(lockman.getLocks(name));
        LockType leastPermissive = LockType.parentLock(child.lockType);
        for (Lock l : parentLocks) {
            if (LockType.substitutable(l.lockType, leastPermissive) && l.transactionNum != transaction.getTransNum()) {
                return true;
            }
        }
        return false;
    }


    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     * @throws InvalidLockException if the lock cannot be released (because doing so would
     *  violate multigranularity locking constraints)
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(BaseTransaction transaction)
    throws NoLockHeldException, InvalidLockException {
        if (readonly) {
            throw new UnsupportedOperationException("Read only");
        }
        for (Object c : children.keySet()) {
            LockContext a = children.get(c);
            for (Lock l : a.lockman.getLocks(a.name)) {
                if (!checkChild(transaction, l)) {
                    throw new InvalidLockException("Invalid Lock.");
                }
            }
        }
        lockman.release(transaction, name);
        LockContext curr = parent;
        while (curr != null) {
            curr.numChildLocks.put(transaction.getTransNum(), curr.numChildLocks.get(transaction.getTransNum()) - 1);
            curr = curr.parent;
        }
    }



    /**
     * Promote TRANSACTION's lock to NEWLOCKTYPE.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a NEWLOCKTYPE lock
     * @throws NoLockHeldException if TRANSACTION has no lock
     * @throws InvalidLockException if the requested lock type is not a promotion or promoting
     * would cause the lock manager to enter an invalid state (e.g. IS(parent), X(child)). A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(BaseTransaction transaction, LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        if (readonly) {
            throw new UnsupportedOperationException("Context is read only");
        }
        if (!parentSubstitutable(transaction, newLockType)) {
            throw new InvalidLockException("No parent lock");
        }
        lockman.promote(transaction, name, newLockType);
    }

    public void setChildrenToNL(BaseTransaction transaction, LockContext lc) {
        List<Lock> currLocks = lockman.getLocks(lc.name);
        Lock lock = null;
        boolean found = false;
        for (Lock l : currLocks) {
            if (l.transactionNum == transaction.getTransNum()) {
                found = true;
                lock = l;
            }
        }
        if (!found) {
            return;
        }
        lock.lockType = LockType.NL;
        LockContext curr = lc.parent;
        while (curr != null) {
            curr.numChildLocks.put(transaction.getTransNum(), curr.numChildLocks.get(transaction.getTransNum()) - 1);
            curr = curr.parent;
        }
        if (lc.children.size() == 0) {
            return;
        } else {
            for (Object c : lc.children.keySet()) {
                setChildrenToNL(transaction, lc.children.get(c));
            }
        }
    }

    /**
     * Escalate TRANSACTION's lock from descendants of this context to this level, using either
     * an S or X lock. There should be no descendant locks after this
     * call, and every operation valid on descendants of this context before this call
     * must still be valid. You should only make *one* mutating call to the lock manager,
     * and should only request information about TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *      IX(database) IX(table1) S(table2) S(table1 page3) X(table1 page5)
     * then after table1Context.escalate(transaction) is called, we should have:
     *      IX(database) X(table1) S(table2)
     *
     * You should not make any mutating calls if the locks held by the transaction do not change
     * (such as when you call escalate multiple times in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all relevant contexts, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if TRANSACTION has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(BaseTransaction transaction) throws NoLockHeldException {
        if (readonly) {
            throw new UnsupportedOperationException("Read only");
        }
        List<Lock> currLocks = lockman.getLocks(name);
        boolean found = false;
        boolean onlyS = true;
        Lock lock = null;
        for (Lock l : currLocks) {
            if (l.transactionNum == transaction.getTransNum()) {
                found = true;
                lock = l;
                if (l.lockType == LockType.SIX || l.lockType == LockType.X || l.lockType == LockType.IX) {
                    onlyS = false;
                }
            }
        }
        if (!found) {
            throw new NoLockHeldException("Transaction has no lock at this level");
        }
        if (onlyS) {
            lock.lockType = LockType.S;
        } else {
            lock.lockType = LockType.X;
        }
        for (Object c : this.children.keySet()) {
            setChildrenToNL(transaction, this.children.get(c));
        }
    }

    /**
     * Gets the type of lock that the transaction has at this level, either implicitly
     * (e.g. explicit S lock at higher level implies S lock at this level) or explicitly.
     * Returns NL if there is no explicit nor implicit lock.
     */
    public LockType getEffectiveLockType(BaseTransaction transaction) {
        if (transaction == null) {
            return LockType.NL;
        }
        LockContext currContext = this;
        LockType prevLockType = null;
        LockType currLockType = null;
        while (currContext != null && currLockType != LockType.SIX && currLockType != LockType.IS && currLockType != LockType.IX) {
            for (Lock c : lockman.getLocks(currContext.name)) {
                if (c.transactionNum == transaction.getTransNum()) {
                    prevLockType = currLockType;
                    currLockType = c.lockType;
                }
            }
            currContext = currContext.parent;
        }
        if (currLockType == LockType.S || currLockType == LockType.X) {
            return currLockType;
        }
        if (currLockType == LockType.SIX) {
            return LockType.S;
        } else {
            if (prevLockType == null) {
                return LockType.NL;
            } else {
                return prevLockType;
            }
        }
    }

    /**
     * Get the type of lock that TRANSACTION holds at this level, or NL if no lock is held at this level.
     */
    public LockType getExplicitLockType(BaseTransaction transaction) {
        if (transaction == null) {
            return LockType.NL;
        }
        List<Lock> currLocks = lockman.getLocks(name);
        for (Lock l : currLocks) {
            if (l.transactionNum == transaction.getTransNum()) {
                return l.lockType;
            }
        }
        return LockType.NL;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this context
     * to be readonly. This is used for indices and temporary tables (where
     * we disallow finer-grain locks), the former due to complexity locking
     * B+ trees, and the latter due to the fact that temporary tables are only
     * accessible to one transaction, so finer-grain locks make no sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name NAME.
     */
    public LockContext childContext(Object name) {
        LockContext temp = new LockContext(lockman, this, name,
                                           this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) {
            child = temp;
        }
        return child;
    }

    /**
     * Sets the capacity (number of children).
     */
    public void capacity(int capacity) {
        this.capacity = capacity;
    }

    /**
     * Gets the capacity. Defaults to number of child contexts if never explicitly set.
     */
    public int capacity() {
        return this.capacity == 0 ? this.children.size() : this.capacity;
    }

    /**
     * Gets the saturation (number of locks held on children / number of children) for
     * a single transaction. Saturation is 0 if number of children is 0.
     */
    public double saturation(BaseTransaction transaction) {
        if (transaction == null || capacity() == 0) {
            return 0.0;
        }
        return ((double) numChildLocks.getOrDefault(transaction.getTransNum(), 0)) / capacity();
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

