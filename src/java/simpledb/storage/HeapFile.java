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
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private BufferPool bufferPool;
    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        bufferPool = new BufferPool();
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
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
       return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int offset = pid.getPageNumber() * BufferPool.getPageSize();
        Page page;
        BufferedInputStream is;
        try {
            FileInputStream fs = new FileInputStream(file);
            // offset是pid这个页之前的页
            fs.skip(offset);
            is = new BufferedInputStream(fs);
            byte[] pageContent = new byte[BufferPool.getPageSize()];
            is.read(pageContent, 0, BufferPool.getPageSize());
            page = new HeapPage((HeapPageId) pid, pageContent);
        } catch (Exception e) {
            throw new UnsupportedOperationException();
        }
        return page;
    }

    // see DbFile.java for javadocs
    // 注意：要用RandomAccessFile实现指定写入位置的操作，RandomAccessFile在写入空间不够时，可以直接append
    public void writePage(Page page) throws IOException {
        int offset = page.getId().getPageNumber() * BufferPool.getPageSize();
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");) {
            randomAccessFile.seek(offset);
            randomAccessFile.write(page.getPageData());
        } catch (Exception e) {
            throw new IOException();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        int index = 0;
        HeapPage page = (HeapPage) readPage(new HeapPageId(getId(), index));
        while (page.getNumEmptySlots() == 0 && index < numPages()) {
            page = (HeapPage) readPage(new HeapPageId(getId(), ++index));
        }
        if (index >= numPages()) {
            page = new HeapPage(new HeapPageId(getId(), index), HeapPage.createEmptyPageData());
        }
        page.insertTuple(t);
        writePage(page);
        return Collections.singletonList(page);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        int index = 0;
        boolean deleted = false;
        HeapPage page = (HeapPage) readPage(new HeapPageId(getId(), index));
        while (!deleted && index < numPages()) {
            if (page.getNumEmptySlots() < page.numSlots) {
                try {
                    page.deleteTuple(t);
                    deleted = true;
                } catch (Exception e) {
                    index++;
                    page = (HeapPage) readPage(new HeapPageId(getId(), index));
                }
            }
        }
        if (deleted) {
            try {
                writePage(page);
            } catch (Exception e) {
                throw new DbException("delete error!");
            }
            return new ArrayList<>(Collections.singletonList(page));
        } else {
            throw new DbException("delete error!");
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {

        return new DbFileIterator() {

            int pageNumberCursor = -1;

            Iterator<Tuple> curIterator;

            BufferedInputStream is;

            boolean opened;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                try(BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(file))){
                    is = bufferedInputStream;
                    opened = true;
                }catch(Exception e){
                    throw new DbException("open DbFileIterator error");
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(!opened){
                    return false;
                }
                if (curIterator == null || !curIterator.hasNext()) {
                    while (pageNumberCursor < numPages() - 1) {
                        HeapPage page = (HeapPage) bufferPool.getPage(tid, new HeapPageId(getId(), ++pageNumberCursor), Permissions.READ_ONLY);
                        curIterator = page.iterator();
                        if(curIterator.hasNext()){
                            break;
                        }
                    }
                }

                return !(curIterator == null) && curIterator.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(curIterator == null || !curIterator.hasNext() || !opened){
                    throw new NoSuchElementException();
                }
                return curIterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                pageNumberCursor = 0;
                HeapPage heapPage = (HeapPage) bufferPool.getPage(tid, new HeapPageId(getId(), 0), Permissions.READ_ONLY);
                curIterator = heapPage.iterator();
            }

            @Override
            public void close() {
                try{
                    is.close();
                    opened = false;
                }catch (Exception e){

                }
            }
        };
    }

}

