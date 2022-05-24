package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private File file;

    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        Page page;
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "r");
            // start offset
            int start = pid.getPageNumber() * BufferPool.getPageSize();
            byte[] buf = new byte[BufferPool.getPageSize()];
            randomAccessFile.read(buf, start, BufferPool.getPageSize());
            page = new HeapPage((HeapPageId) pid, buf);
            randomAccessFile.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        // total page num = (file size) / (page size)
        return (int) this.file.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        DbFileIterator iterator = null;
        try {
            iterator = new HeapFileIterator(tid, this);
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } catch (DbException e) {
            e.printStackTrace();
        }
        return iterator;
    }

    public class HeapFileIterator extends AbstractDbFileIterator {

        private TransactionId tid;

        private HeapFile heapFile;

        private int pageNow = 0;
        
        private int pageNum = 0;
        
        private Iterator<Tuple> tupleIteratorNow = null;
        
        public HeapFileIterator(TransactionId tid, HeapFile heapFile) throws TransactionAbortedException, DbException {
            this.tid = tid;
            this.heapFile = heapFile;
            this.pageNum = heapFile.numPages();
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if (this.tupleIteratorNow == null) return null;
            if (!tupleIteratorNow.hasNext()) {
                // if this page has been visited
                if (pageNow < pageNum-1) {
                    pageNow++;
                    this.tupleIteratorNow = ((HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(this.heapFile.getId(), pageNum), Permissions.READ_ONLY)).iterator();
                } else{
                    return null;
                }
            }
            return this.tupleIteratorNow.next();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            PageId pageId = new HeapPageId(heapFile.getId(), pageNow);
            this.tupleIteratorNow = ((HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY)).iterator();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.pageNow = 0;
            open();
        }

        @Override
        public void close() {
            super.close();
            this.tupleIteratorNow = null;
        }
    }

}

