package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;

/**
 * Predicate compares tuples to a specified Field value.
 */
public class Predicate implements Serializable {
    // 要比较的字段在tuple中的编号，即此字段是tuple的第几个字段
    int filedNumber;

    // 要满足的field条件，即一个tuple的指定位置字段只有满足了这个field和op的匹配关系，才能通过过滤器
    Field operand;

    //要和field执行的操作
    Op op;

    private static final long serialVersionUID = 1L;

    /** Constants used for return codes in Field.compare */
    public enum Op implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i
         *            a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LIKE)
                return "LIKE";
            if (this == NOT_EQUALS)
                return "<>";
            throw new IllegalStateException("impossible to reach here");
        }

    }

    /**
     * Constructor.
     *
     * @param field
     *            field number of passed in tuples to compare against.
     * @param op
     *            operation to use for comparison
     * @param operand
     *            field value to compare passed in tuples to
     */
    public Predicate(int field, Op op, Field operand) {
        this.filedNumber = field;
        this.op = op;
        this.operand = operand;
    }

    /**
     * @return the field number
     */
    public int getField()
    {
        return filedNumber;
    }

    /**
     * @return the operator
     */
    public Op getOp()
    {
        return op;
    }

    /**
     * @return the operand
     */
    public Field getOperand()
    {
        return operand;
    }

    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific in
     * the constructor. The comparison can be made through Field's compare
     * method.
     *
     * @param t
     *            The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */
    public boolean filter(Tuple t) {
        // 注意，比较的主体是传入的tuple，比如断言内容是 小于4,那么operand是4，op是小于
        // 则传入3的时候，要验证的是3 < 4，即要由oriField来调用compare
        Field oriField = t.getField(filedNumber);
        return oriField.compare(op, operand);
    }

    /**
     * Returns something useful, like "f = field_id op = op_string operand =
     * operand_string"
     */
    public String toString() {
        return "f = " +
                filedNumber +
                " op = " +
                op +
                " operand = " +
                operand;
    }
}
