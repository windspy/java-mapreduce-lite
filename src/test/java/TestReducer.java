import com.mapreducelite.Context;
import com.mapreducelite.Reducer;
import com.mapreducelite.impl.ContextSource;
import com.mapreducelite.type.IntNumber;
import com.mapreducelite.type.MRText;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: yibing.tan
 * Date: 11-8-24
 * Time: 上午11:05
 * To change this template use File | Settings | File Templates.
 */
public class TestReducer extends Reducer<MRText, IntNumber> {
    public TestReducer(Context context){
        super(context);
    }
    protected void reduce(MRText key, Iterable<IntNumber> values) throws IOException, InterruptedException {
        int sum = 0;
        for (IntNumber value : values)
            sum+=value.getValue();
        context.write(key, new IntNumber(Integer.toString(sum)), ContextSource.REDUCE);
    }

}
