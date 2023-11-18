package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
// 在groupField-1的时候，实际就是在所有行中执行group操作，只需要得到一个整张表的计算结果即可
public class Aggregate extends Operator {
    private OpIterator child;
    private int valueFieldNumber;
    private int groupFieldNumber;
    private Aggregator.Op op;

    // 其实就是包装了一下这个，实际的fetchnext方法完全用的就是aggregator的方法
    private Aggregator aggregator;

    private List<Tuple> childTuples = new ArrayList<>();

    private OpIterator it;

    private TupleDesc td;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.groupFieldNumber = gfield;
        this.valueFieldNumber = afield;
        this.op = aop;
        Type valueType = child.getTupleDesc().getFieldType(afield);
        Type groupType = gfield == -1 ? null: child.getTupleDesc().getFieldType(gfield);
        if (valueType.equals(Type.INT_TYPE)) {
            this.aggregator = new IntegerAggregator(gfield, groupType, afield, aop);
        } else {
            this.aggregator = new StringAggregator(gfield, groupType, afield, aop);
        }
        if (gfield != -1) {
            String[] names = new String[]{child.getTupleDesc().getFieldName(gfield), "res"};
            Type[] types = new Type[]{groupType, valueType};
            this.td = new TupleDesc(types, names);
        }else{
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return op == null ? Aggregator.NO_GROUPING : groupFieldNumber;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        return op == null ? null : child.getTupleDesc().getFieldName(groupFieldNumber);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return valueFieldNumber;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(valueFieldNumber);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        child.open();
        while(child.hasNext()){
            aggregator.mergeTupleIntoGroup(child.next());
        }
        it = aggregator.iterator();
        it.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(it.hasNext()){
            return it.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        it.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    public void close() {
       it.close();
       super.close();
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }

}
