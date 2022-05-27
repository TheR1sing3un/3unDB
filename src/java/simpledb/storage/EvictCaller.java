package simpledb.storage;

/**
 * @author TheR1sing3un
 * @date 2022/5/27 22:04
 * @description
 */

public interface EvictCaller {
    void evictCall(Page page);
}
