package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.DatabaseException;

public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        if (a.toString() == "NL" || b.toString() == "NL") {
            return true;
        }
        if (a.toString() == "S") {
            return b.toString() == "S" || b.toString() == "IS" || b.toString() == "NL";
        }
        if (a.toString() == "X") {
            return b.toString() == "NL";
        }
        if (a.toString() == "IS") {
            return b.toString() == "S" || b.toString() == "IS" | b.toString() == "IX";
        }
        if (a.toString() == "IX") {
            return b.toString() == "IX" || b.toString() == "IS";
        }
        if (a.toString() == "SIX") {
            return b.toString() == "SIX";
        }
        return false;
    }

    /**
     * This method returns the least permissive lock on the parent resource
     * that must be held for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        if (a.toString() == "S") {
            return IS;
        }
        if (a.toString() == "X") {
            return IX;
        }
        if (a.toString() == "IS") {
            return IS;
        }
        if (a.toString() == "IX") {
            return IX;
        }
        if (a.toString() == "SIX") {
            return NL;
        }
        return NL;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        if (required.toString() == "S") {
            return substitute.toString() == "X" || substitute.toString() == "SIX" || substitute.toString() == "S";
        }
        if (required.toString() == "X") {
            return substitute.toString() == "X";
        }
        if (required.toString() == "IS") {
            return substitute.toString() == "IX" || substitute.toString() == "IS";
        }
        if (required.toString() == "IX") {
            return substitute.toString() == "SIX" || substitute.toString() == "IX";
        }
        if (required.toString() == "SIX") {
            return substitute.toString() == "SIX";
        }
        if (required.toString() == "NL") {
            return substitute.toString() == "NL";
        }
        return false;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
};

