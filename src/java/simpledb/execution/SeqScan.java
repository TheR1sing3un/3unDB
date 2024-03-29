package simpledb.execution;

import simpledb.common.Database;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private String tableAlias;

    private int tableId;

    private String tableName;

    private TransactionId tid;

    private DbFileIterator iterator = null;

    private TupleDesc tupleDesc;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid = tid;
        this.tableId = tableid;
        this.tableAlias = tableAlias;
        this.tableName = Database.getCatalog().getTableName(tableid);
        TupleDesc tupleDesc = Database.getCatalog().getTupleDesc(this.tableId);
        String tableName = getTableName();
        if (!tableName.equals(this.tableAlias)) {
            Type[] types = new Type[tupleDesc.numFields()];
            String[] fieldNames = new String[tupleDesc.numFields()];
            int index = 0;
            TupleDesc.TDItem item = null;
            Iterator<TupleDesc.TDItem> iterator = tupleDesc.iterator();
            while (iterator.hasNext()) {
                item = iterator.next();
                types[index] = item.fieldType == Type.INT_TYPE ? Type.INT_TYPE : Type.STRING_TYPE;
                fieldNames[index++] = this.tableAlias + "." + item.fieldName;
            }
            this.tupleDesc = new TupleDesc(types, fieldNames);
        } else {
            this.tupleDesc = tupleDesc;
        }
    }

    /**
     * @return return the table name of the table the operator scans. This should
     * be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return this.tableName;
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        // some code goes here
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     *
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableId = tableid;
        this.tableAlias = tableAlias;
        TupleDesc td = this.tupleDesc;
        if (!this.tableName.equals(this.tableAlias)) {
            Type[] types = new Type[td.numFields()];
            String[] fieldNames = new String[td.numFields()];
            int index = 0;
            TupleDesc.TDItem item = null;
            Iterator<TupleDesc.TDItem> iterator = td.iterator();
            if (iterator.hasNext()) {
                item = iterator.next();
                types[index] = item.fieldType;
                fieldNames[index++] = this.tableAlias + "." + item.fieldName;
            }
            this.tupleDesc = new TupleDesc(types, fieldNames);
        }
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.iterator = Database.getCatalog().getDatabaseFile(this.tableId).iterator(this.tid);
        this.iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return this.iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException {
        // some code goes here
        return this.iterator.next();
    }

    public void close() {
        // some code goes here
        this.iterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException, TransactionAbortedException {
        // some code goes here
        this.iterator.rewind();
    }
}
