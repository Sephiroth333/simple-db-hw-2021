package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {
    private int groupFieldNumber;
    private Type groupFieldType;
    private int valueFieldNumber;
    private Aggregator.Op op;

    private TupleDesc td;

    private Map<Field, Record> map = new HashMap<>();

    private List<Tuple> tupleList = new ArrayList<>();

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.groupFieldNumber = gbfield;
        this.groupFieldType = gbfieldtype;
        this.valueFieldNumber = afield;
        this.op = what;
        if (what != null) {
            Type[] types = new Type[]{gbfieldtype, Type.INT_TYPE};
            this.td = new TupleDesc(types);
        } else {
            this.td = new TupleDesc(new Type[]{gbfieldtype});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (op == null) {
            tupleList.add(tup);
        } else {
            Field gField = tup.getField(groupFieldNumber);
            int value = ((IntField) tup.getField(valueFieldNumber)).getValue();
            Record record;
            if (map.containsKey(gField)) {
                record = calculate(value, map.get(gField));
            } else {
                record = new Record(value, op);
            }
            map.put(gField, record);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        return new OpIterator() {
            Iterator<Tuple> it;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                if (op != null) {
                    for (Map.Entry<Field, Record> entry : map.entrySet()) {
                        Tuple tuple = new Tuple(td);
                        tuple.setField(0, entry.getKey());
                        tuple.setField(1, new IntField(entry.getValue().groupRes));
                        tupleList.add(tuple);
                    }
                }
                it = tupleList.iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return it.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                return it.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                it = tupleList.iterator();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return td;
            }

            @Override
            public void close() {
                it = null;
            }
        };
    }


    private Record calculate(int curValue,Record record){
        switch (op){
            case MIN:
                record.groupRes = Math.min(curValue,record.groupRes);
                break;
            case MAX:
                record.groupRes = Math.max(curValue,record.groupRes);
                break;
            case SUM:
                record.sum += curValue;
                record.groupRes = record.sum;
                break;
            case AVG:
                record.count +=1;
                record.sum +=curValue;
                record.groupRes = record.avg();
                break;
            case COUNT:
                record.count +=1;
                record.groupRes = record.count;
                break;
            default:
        }
        return record;
    }

    static class Record{
        int groupRes;
        int sum;

        int count;

        int avg() {
            return this.sum / this.count;
        }

        Record(int value, Op op){
            this.sum = value;
            this.count = 1;
            switch (op){
                case MIN:
                case MAX:
                case SUM:
                case AVG:
                    this.groupRes = value;
                    break;
                case COUNT:
                    this.groupRes = 1;
            }
        }

    }

}
