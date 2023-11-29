package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.LinkedHashMap;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private LinkedHashMap<Integer,Integer> bucketMap;

    private int bucketWidth;

    private int min;

    private int max;

    private int recordCount = 0;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        int bucketNum = Math.min(buckets, max - min+1);
        // 桶宽度要向上取整
        this.bucketWidth = (int) Math.ceil((double) (max - min+1) / bucketNum);
        this.min = min;
        this.max = max;
        this.bucketMap = new LinkedHashMap<>();
        for (int i = 0; i < bucketNum; i++) {
            bucketMap.put(i, 0);
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int pos = getBucketPos(v);
        bucketMap.put(pos, bucketMap.get(pos) + 1);
        recordCount++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int hitCount;
        // 桶索引，从0开始
        int bucketPos = getBucketPos(v);
        // 此桶索引的对应具体数值
        int realPos = min + bucketPos * bucketWidth;
        switch (op){
            case EQUALS:
            case LIKE:
                if (bucketPos == -1 || bucketPos >= bucketMap.size()) {
                    hitCount = 0;
                } else {
                    hitCount = bucketMap.get(bucketPos) / bucketWidth;
                }
                break;
            case LESS_THAN_OR_EQ:
                if(bucketPos == -1 || bucketPos >= bucketMap.size()){
                    hitCount = bucketPos == -1 ? 0 : recordCount;
                }else{
                    // 需要重点考虑的问题是，当包含等于的时候，如何把这个值本身的记录条数加进去
                    hitCount = (v - realPos + 1) / bucketWidth * bucketMap.get(bucketPos);
                    for (int pos : bucketMap.keySet()) {
                        if (pos < bucketPos) {
                            hitCount += bucketMap.get(pos);
                        }
                    }
                }

                break;
            case LESS_THAN:
                if(bucketPos == -1 || bucketPos >= bucketMap.size()){
                    hitCount = bucketPos == -1 ? 0 : recordCount;
                }else{
                    hitCount = (v - realPos) / bucketWidth * bucketMap.get(bucketPos);
                    for (int pos : bucketMap.keySet()) {
                        if (pos < bucketPos) {
                            hitCount += bucketMap.get(pos);
                        }
                    }
                }
                break;
            case GREATER_THAN_OR_EQ:
                if(bucketPos == -1 || bucketPos >= bucketMap.size()){
                    hitCount = bucketPos >= bucketMap.size() ? 0 : recordCount;
                }else{
                    hitCount = (realPos + bucketWidth - v) / bucketWidth * bucketMap.get(bucketPos);
                    for (int pos : bucketMap.keySet()) {
                        if (pos > bucketPos) {
                            hitCount += bucketMap.get(pos);
                        }
                    }
                }
                break;
            case GREATER_THAN:
                if(bucketPos == -1 || bucketPos >= bucketMap.size()){
                    hitCount = bucketPos >= bucketMap.size() ? 0 : recordCount;
                }else{
                    hitCount = (realPos + bucketWidth - v -1) / bucketWidth * bucketMap.get(bucketPos);
                    for (int pos : bucketMap.keySet()) {
                        if (pos > bucketPos) {
                            hitCount += bucketMap.get(pos);
                        }
                    }
                }

                break;
            case NOT_EQUALS:
                if(bucketPos == -1 || bucketPos >= bucketMap.size()){
                    hitCount = recordCount;
                }else{
                    hitCount = recordCount-bucketMap.get(bucketPos)/bucketWidth;
                }
                break;
            default:
                hitCount = recordCount;
        }
        return (double) hitCount /recordCount;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        int hitCount = 0;
        for(int count: bucketMap.values()){
            hitCount += count/bucketWidth;
        }
        return (double) (hitCount / bucketMap.size()) /recordCount;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        return "min: " + min + "\n" +
                "max: " + max + "\n" +
                "record count: " + recordCount + "\n" +
                "bucket width: " + bucketWidth + "\n" +
                "bucket map: " + bucketMap;

    }

    private int getBucketPos(int v) {
        if (v < min) {
            return -1;
        }
        if (v > max) {
            return bucketMap.size();
        }
        return (v - min) / bucketWidth;
    }

}
