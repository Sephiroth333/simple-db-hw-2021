package simpledb.storage;

import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    // 页id和该页在pageQueue中的索引
    private final Map<PageId, Integer> pageMap;

    private LinkedList<Page> pageQueue;

    private final int maxPage;

    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        pageMap = new HashMap<>();
        pageQueue = new LinkedList<>();
        maxPage = numPages;
    }

    public BufferPool() {
        this(DEFAULT_PAGES);
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        if (pageMap.containsKey(pid)) {
            return pageQueue.get(pageMap.get(pid));
        } else{
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            if(pageMap.size() >= maxPage){
                evictPage();
            }
            updateCache(page);
            return page;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // 插入操作在没有达到刷盘时机时，tuple只写入bufferpool就行，不需要真的写入磁盘
        HeapPage opPage = null;
        for (int index : pageMap.values()) {
            Page page = pageQueue.get(index);
            if(page.getId().getTableId() == tableId){
                HeapPage heapPage = (HeapPage) page;
                if (heapPage.getNumEmptySlots() > 0) {
                    opPage = heapPage;
                    break;
                }
            }
        }
        if (opPage == null) {
            HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
            // pages是本次insert影响到的页集合，是已经执行执行了insert操作后的页
            List<Page> pages = heapFile.insertTuple(tid, t);
            for (Page page : pages) {
                if (pageMap.size() >= maxPage) {
                    evictPage();
                }
                updateCache(page);
            }
        } else {
            opPage.insertTuple(t);
            opPage.markDirty(true, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        HeapPage opPage = null;
        int tableId = t.getRecordId().getPageId().getTableId();
        for (Integer index : pageMap.values()) {
            Page page = pageQueue.get(index);
            if (page.getId().getTableId() == tableId) {
                HeapPage heapPage = (HeapPage) page;
                Iterator<Tuple> it = heapPage.iterator();
                while (it.hasNext()) {
                    if (it.next().equals(t)) {
                        opPage = heapPage;
                        break;
                    }
                }
                if (opPage != null) {
                    break;
                }
            }
        }
        if (opPage == null) {
            List<Page> pages = Database.getCatalog().getDatabaseFile(tableId).deleteTuple(tid, t);
            for (Page page : pages) {
                if (pageMap.size() >= maxPage) {
                    evictPage();
                }
                updateCache(page);
            }
        } else {
            opPage.deleteTuple(t);
            opPage.markDirty(true, tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        List<PageId> pageIds = new ArrayList<>(pageMap.keySet());
        for (PageId pageId : pageIds) {
            flushPage(pageId);
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        if (pageMap.containsKey(pid)) {
            int index = pageMap.get(pid);
            pageQueue.remove(index);
            pageMap.remove(pid);
        }
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        if (pageMap.containsKey(pid)) {
            Page page = pageQueue.get(pageMap.get(pid));
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            int index = pageMap.get(pid);
            pageQueue.remove(index);
            pageMap.remove(pid);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        Page page = pageQueue.removeLast();
        pageMap.remove(page.getId());
    }

    private void updateCache(Page page){
        if (pageMap.containsKey(page.getId())) {
            int index = pageMap.get(page.getId());
            pageQueue.remove(index);
        }
        pageQueue.addFirst(page);
        pageMap.put(page.getId(), 0);
    }

}
