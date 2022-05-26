package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.Arrays;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    // the index of the field which is the basement of grouping
    private int groupByFieldIndex;

    // the type of the field which is the basement of grouping
    private Type groupByFiledType;

    // the index of the field which is the basement of aggregate
    private int aggregateFiledIndex;

    private TupleDesc td;

    // option
    private Op op;

    private HashMap<Field, Tuple> tupleWithGroup;

    private Tuple tuple;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (!what.equals(Op.COUNT)) {
            throw new IllegalArgumentException();
        }
        this.groupByFieldIndex = gbfield;
        this.groupByFiledType = gbfieldtype;
        this.aggregateFiledIndex = afield;
        this.op = what;
        if (gbfield == NO_GROUPING) {
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
            this.tuple = new Tuple(this.td);
            this.tuple.setField(0, new IntField(0));
        } else {
            this.td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
            this.tupleWithGroup = new HashMap<>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (this.groupByFieldIndex == NO_GROUPING) {
            // no grouping
            int oldVal = ((IntField) this.tuple.getField(0)).getVal();
            this.tuple.setField(0, new IntField(oldVal + 1));
            return;
        }
        // grouping
        Field fieldGroup = tup.getField(this.groupByFieldIndex);
        if (this.tupleWithGroup.containsKey(fieldGroup)) {
            // if contains
            int oldVal = ((IntField) this.tupleWithGroup.get(fieldGroup).getField(1)).getVal();
            this.tupleWithGroup.get(fieldGroup).setField(1, new IntField(oldVal + 1));
        } else {
            // if not, init a tuple
            Tuple tuple = new Tuple(this.td);
            tuple.setField(0, fieldGroup);
            tuple.setField(1, new IntField(1));
            this.tupleWithGroup.put(fieldGroup, tuple);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        if (this.groupByFieldIndex == NO_GROUPING) {
            return new TupleIterator(this.td, Arrays.asList(this.tuple));
        } else {
            return new TupleIterator(this.td, this.tupleWithGroup.values());
        }
    }

    @Override
    public TupleDesc getTd() {
        return this.td;
    }

}
