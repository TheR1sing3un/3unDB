package simpledb;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.SeqScan;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.File;

/**
 * @author TheR1sing3un
 * @date 2022/5/25 14:27
 * @description
 */

public class test {

    public static void main(String[] args) {
        // construct a 3-column table schema
        Type[] types = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String[] names = new String[]{"id", "age", "score"};
        TupleDesc tupleDesc = new TupleDesc(types, names);
        // create the table, associate it with data.dat
        HeapFile heapFile = new HeapFile(new File("data.dat"), tupleDesc);
        // add it to catalog
        Database.getCatalog().addTable(heapFile, "student");
        TransactionId tid = new TransactionId();
        SeqScan scan = new SeqScan(tid, heapFile.getId(), "student");
        try {
            // open the iterator
            scan.open();
            // print all tuples of this table
            while (scan.hasNext()) {
                System.out.println(scan.next());
            }
            // complete the transaction
            Database.getBufferPool().transactionComplete(tid);
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } finally {
            // close the iterator
            scan.close();
        }

    }
}
