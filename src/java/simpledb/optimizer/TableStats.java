package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionId;
import sun.java2d.marlin.stats.Histogram;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private int tupleSum;

    private Set<PageId> pageSet;

    private TupleDesc tupleDesc;

    private int ioCostPerPage;

    private ConcurrentHashMap<Integer, IntHistogram> intHisMap = new ConcurrentHashMap<>();

    private ConcurrentHashMap<Integer, StringHistogram> strHisMap = new ConcurrentHashMap<>();



    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    // 是不是就是根据field的类型构建每个field的直方图存在对象中，遍历了表两次
    public TableStats(int tableid, int ioCostPerPage) {
        this.ioCostPerPage = ioCostPerPage;
        this.tupleDesc = Database.getCatalog().getTupleDesc(tableid);
        this.pageSet = new HashSet<>();

        TransactionId transactionId = new TransactionId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableid);
        DbFileIterator it =  dbFile.iterator(transactionId);
        Map<Integer,Integer> minMap = new HashMap<>();
        Map<Integer,Integer> maxMap = new HashMap<>();
        try {
            it.open();
            while (it.hasNext()) {
                // 遍历一次，计算总tuple数和page数，算出来int字段的最大最小值
                Tuple tuple = it.next();
                pageSet.add(tuple.getRecordId().getPageId());
                tupleSum++;
                for (int i = 0; i < tupleDesc.numFields(); i++) {
                    Field field = tuple.getField(i);
                    if (field.getType().equals(Type.INT_TYPE)) {
                        int value = ((IntField) field).getValue();
                        minMap.putIfAbsent(i, value);
                        minMap.put(i, Math.min(minMap.get(i), value));
                        maxMap.putIfAbsent(i, value);
                        maxMap.put(i, Math.max(maxMap.get(i), value));
                    }
                }
            }
            for (int i = 0; i < tupleDesc.numFields(); i++) {
                Type type = tupleDesc.getFieldType(i);
                if (type.equals(Type.INT_TYPE)) {
                    intHisMap.put(i, new IntHistogram(NUM_HIST_BINS, minMap.get(i), maxMap.get(i)));
                }else{
                    strHisMap.put(i, new StringHistogram(NUM_HIST_BINS));
                }
            }
            it.rewind();
            while (it.hasNext()) {
                // 遍历一次，计算总tuple数和page数，算出来int字段的最大最小值
                Tuple tuple = it.next();
                for (int i = 0; i < tupleDesc.numFields(); i++) {
                    Field field = tuple.getField(i);
                    if (field.getType().equals(Type.INT_TYPE)) {
                        IntField intField = (IntField) field;
                        IntHistogram intHistogram = intHisMap.get(i);
                        intHistogram.addValue(intField.getValue());
                    } else {
                        StringField strField = (StringField) field;
                        StringHistogram strHistogram = strHisMap.get(i);
                        strHistogram.addValue(strField.getValue());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    // 这个应该是页数 x 每页的扫描成本即可
    public double estimateScanCost() {
        return pageSet.size() * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    // 这个应该是总tuple数 x selectivityFactor就行
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) (tupleSum * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param fieldNumber
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    // 用直方图里面每个桶中单个值的平均期望tuple数加起来 除以 总tuple数？
    public double avgSelectivity(int fieldNumber, Predicate.Op op) {
        Type type = tupleDesc.getFieldType(fieldNumber);
        if (type.equals(Type.INT_TYPE)) {
            IntHistogram intHistogram = intHisMap.get(fieldNumber);
            return intHistogram.avgSelectivity();
        } else {
            StringHistogram stringHistogram = strHisMap.get(fieldNumber);
            return stringHistogram.avgSelectivity();
        }
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    // 直接调用直方图的方法即可
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        Type type = tupleDesc.getFieldType(field);
        if (type.equals(Type.INT_TYPE)) {
            IntHistogram intHistogram = intHisMap.get(field);
            return intHistogram.estimateSelectivity(op, ((IntField) constant).getValue());
        } else {
            StringHistogram stringHistogram = strHisMap.get(field);
            return stringHistogram.estimateSelectivity(op, ((StringField) constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    // 总元祖数
    public int totalTuples() {
        return tupleSum;
    }

}
