package simpledb.storage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private TupleDesc tupleDesc;

    private Field[] fields;

    private RecordId recordId;

    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        int fieldSize = td.numFields();
        if (fieldSize > 0) {
            tupleDesc = td;
            Field[] tmpFields = new Field[fieldSize];
            for (int i = 0; i < fieldSize; i++) {
                tmpFields[i] = td.getFieldType(i).genEmptyField();
            }
            fields = tmpFields;
        } else {
            throw new UnsupportedOperationException("cons tuple error! empty init TupleDesc");
        }
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     * be null.
     */
    public RecordId getRecordId() {
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        if (i >= fields.length) {
            throw new UnsupportedOperationException("index large than field size");
        }
        Field field = fields[i];
        if (field.getType() != f.getType()) {
            throw new UnsupportedOperationException("error field type");
        }
        fields[i] = f;
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        if (i >= fields.length) {
            throw new UnsupportedOperationException("index large than field size");
        }
        return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p>
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Field field : fields) {
            sb.append(field).append(" ");
        }
        return sb.toString().trim();
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        return Arrays.stream(fields).iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        int fieldSize = td.getSize();
        if (fieldSize > 1) {
            tupleDesc = td;
            Field[] tmpFields = new Field[fieldSize];
            for (int i = 0; i < fieldSize; i++) {
                tmpFields[i] = td.getFieldType(i).genEmptyField();
            }
            fields = tmpFields;
        }
        throw new UnsupportedOperationException("cons tuple error! empty init TupleDesc");
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == this){
            return true;
        }
        if(obj instanceof Tuple){
            Tuple t = (Tuple) obj;
            if(t.getTupleDesc().equals(this.getTupleDesc())){
                for(int i=0;i< fields.length;i++){
                    if(!getField(i).equals(t.getField(i))){
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }
}
