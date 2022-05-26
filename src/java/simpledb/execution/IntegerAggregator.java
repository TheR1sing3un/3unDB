package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    // the index of the field which is the basement of grouping
    private int groupByFieldIndex;

    // the type of the field which is the basement of grouping
    private Type groupByFiledType;

    // the index of the field which is the basement of aggregate
    private int aggregateFiledIndex;

    // option
    private Op op;

    private HashMap<Field, IntegerAggregatorItem> itemWithGroup;

    private IntegerAggregatorItem item;

    private TupleDesc td;

    private class IntegerAggregatorItem {

        private int sum = 0;

        private int count = 0;

        private int avg = 0;

        private int min = Integer.MAX_VALUE;

        private int max = Integer.MIN_VALUE;

        private Op op;

        public IntegerAggregatorItem(Op op) {
            this.op = op;
        }

        public void merge(int val) {
            count++;
            sum += val;
            avg = sum / count;
            min = Math.min(val, min);
            max = Math.max(val, max);
        }

        public int result() {
            switch (this.op) {
                case MIN: {
                    return this.min;
                }
                case MAX: {
                    return this.max;
                }
                case SUM: {
                    return this.sum;
                }
                case AVG: {
                    return this.avg;
                }
                case COUNT: {
                    return this.count;
                }
                default: {
                    return -1;
                }
            }
        }
    }

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.groupByFieldIndex = gbfield;
        this.groupByFiledType = gbfieldtype;
        this.aggregateFiledIndex = afield;
        this.op = what;
        if (gbfield == NO_GROUPING) {
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
            this.item = new IntegerAggregatorItem(this.op);
        } else {
            this.td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
            this.itemWithGroup = new HashMap<>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        // if no grouping
        if (this.groupByFieldIndex == NO_GROUPING) {
            this.item.merge(((IntField) tup.getField(this.aggregateFiledIndex)).getVal());
            return;
        }
        // if grouping
        Field fieldGroup = tup.getField(this.groupByFieldIndex);
        IntField fieldAggregate = (IntField) tup.getField(this.aggregateFiledIndex);
        if (this.itemWithGroup.containsKey(fieldGroup)) {
            // if contains, add it
            this.itemWithGroup.get(fieldGroup).merge(fieldAggregate.getVal());
        } else {
            // if not, init a group
            IntegerAggregatorItem value = new IntegerAggregatorItem(this.op);
            value.merge(fieldAggregate.getVal());
            this.itemWithGroup.put(fieldGroup, value);
        }
    }


    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> list;
        if (this.groupByFieldIndex == NO_GROUPING) {
            Tuple tuple = new Tuple(this.td);
            tuple.setField(0, new IntField(this.item.result()));
            list = Arrays.asList(tuple);
        } else {
            // grouping
            list = new ArrayList<>(this.itemWithGroup.size());
            Set<Map.Entry<Field, IntegerAggregatorItem>> entries = this.itemWithGroup.entrySet();
            for (Map.Entry<Field, IntegerAggregatorItem> entry : entries) {
                Tuple tuple = new Tuple(this.td);
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(entry.getValue().result()));
                list.add(tuple);
            }
        }
        return new TupleIterator(this.td, list);
    }

    @Override
    public TupleDesc getTd() {
        return this.td;
    }

}
