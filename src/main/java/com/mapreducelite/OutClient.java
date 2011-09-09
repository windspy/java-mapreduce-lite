package com.mapreducelite;

import com.mapreducelite.impl.ContextSource;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: yibing.tan
 * Date: 11-8-24
 * Time: 下午4:11
 * To change this template use File | Settings | File Templates.
 */
public interface OutClient<KEY,VALUE> {
    public void write(KEY key, VALUE value, ContextSource source);

    public Set<KEY> keys(ContextSource source);

    public Set<VALUE> values(KEY ckey, ContextSource source);

    public Map<KEY, Set<VALUE>> getResults(ContextSource source);

    public void clearAll();
}
