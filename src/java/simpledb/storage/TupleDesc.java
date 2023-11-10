package simpledb.storage;

import com.sun.tools.javac.util.StringUtils;
import simpledb.common.CommonUtil;
import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    private volatile List<TDItem> tdItemList = new CopyOnWriteArrayList<>();

    private volatile int tupleByteSize = 0;

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return tdItemList.stream().iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        int tmpTupleByteSize = 0;
        List<TDItem> tmpTdItemList = new CopyOnWriteArrayList<>();
        if (typeAr.length != 0 && fieldAr.length != 0 && typeAr.length == fieldAr.length) {
            for (int i = 0; i < typeAr.length; i++) {
                String fieldName = fieldAr[i] == null ? "" : fieldAr[i];
                tmpTdItemList.add(new TDItem(typeAr[i], fieldName));
                tmpTupleByteSize += typeAr[i].getLen();
            }
            this.tdItemList = tmpTdItemList;
            this.tupleByteSize = tmpTupleByteSize;
        } else {
            throw new UnsupportedOperationException("init TupleDesc fail!");
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this(typeAr, new String[typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return tdItemList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i >= tdItemList.size()) {
            throw new NoSuchElementException();
        }
        return tdItemList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i >= tdItemList.size()) {
            throw new NoSuchElementException();
        }
        return tdItemList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < tdItemList.size(); i++) {
            TDItem tdItem = tdItemList.get(i);
            if (CommonUtil.isStringEqual(name, tdItem.fieldName)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        return tupleByteSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        if (td1 == null || td2 == null) {
            return td1 == null ? td2 : td1;
        }
        int size1 = td1.numFields();
        int size2 = td2.numFields();
        Type[] types = new Type[size1 + size2];
        String[] fieldNames = new String[size1 + size2];
        for (int i = 0; i < size1 + size2; i++) {
            TupleDesc tmpTd;
            int oriIndex = i;
            if (i < size1) {
                tmpTd = td1;
            } else {
                tmpTd = td2;
                oriIndex = i - size1;
            }
            types[i] = tmpTd.getFieldType(oriIndex);
            fieldNames[i] = tmpTd.getFieldName(oriIndex);
        }
        return new TupleDesc(types, fieldNames);

    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof TupleDesc) {
            TupleDesc tupleDesc = (TupleDesc) o;
            if (this.numFields() == tupleDesc.numFields()) {
                for (int i = 0; i < this.tdItemList.size(); i++) {
                    if (!tupleDesc.getFieldType(i).equals(this.getFieldType(i))) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        return tdItemList.hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        return tdItemList.toString();
    }
}
