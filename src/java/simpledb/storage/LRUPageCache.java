package simpledb.storage;

import java.util.*;
import java.util.function.Function;

/**
 * @author TheR1sing3un
 * @date 2022/5/27 21:32
 * @description
 */

public class LRUPageCache {

    private Map<PageId, PagePair> map;

    private LinkedList<PagePair> pages;

    private EvictCaller evictCaller;

    private class PagePair {
        public PageId pageId;

        public Page page;

        public PagePair(PageId pageId, Page page) {
            this.pageId = pageId;
            this.page = page;
        }
    }

    private int capacity;

    public LRUPageCache(int capacity, EvictCaller evictCaller) {
        this.capacity = capacity;
        this.map = new HashMap<>();
        this.pages = new LinkedList<>();
        this.evictCaller = evictCaller;
    }

    public Page get(PageId pageId) {
        if (this.map.containsKey(pageId)) {
            // put it to first
            PagePair pagePair = this.map.get(pageId);
            this.pages.remove(pagePair);
            this.pages.addFirst(pagePair);
            return pagePair.page;
        }
        return null;
    }

    public boolean containsKey(PageId pageId) {
        return this.map.containsKey(pageId);
    }

    public void put(PageId pageId, Page page) {
        // if contain
        if (this.map.containsKey(pageId)) {
            // remove it
            PagePair pagePair = this.map.get(pageId);
            this.pages.remove(pagePair);
        }
        while (this.pages.size() >= this.capacity) {
            // remove the last
            PagePair pagePair = this.pages.removeLast();
            this.map.remove(pagePair.pageId);
            this.evictCaller.evictCall(pagePair.page);
        }
        PagePair pagePair = new PagePair(pageId, page);
        this.map.put(pageId, pagePair);
        this.pages.addFirst(pagePair);
    }

    public void remove(PageId pageId) {
        PagePair pair = this.map.remove(pageId);
        this.pages.remove(pair);
    }

    public List<Page> values() {
        ArrayList<Page> list = new ArrayList(this.pages.size());
        for (PagePair pagePair : this.pages) {
            list.add(pagePair.page);
        }
        return list;
    }
}
