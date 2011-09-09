import com.mapreducelite.Context;
import com.mapreducelite.Mapper;
import com.mapreducelite.impl.ContextSource;
import com.mapreducelite.type.IntNumber;
import com.mapreducelite.type.MRText;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: yibing.tan
 * Date: 11-8-24
 * Time: 上午11:05
 * To change this template use File | Settings | File Templates.
 */
public class TestMapper extends Mapper<MRText, IntNumber> {

    public TestMapper(List<InputStream> fsinputs, Context context) {
        super(fsinputs, context);
    }

    protected void map(String line) throws IOException, InterruptedException {
        if (line == null || "".equals(line.trim())) return;

        String[] values = line.split(" ");

        if (values.length<1) return;

        for (int i=0;i<values.length;i++)
            context.write(new MRText(values[i]),new IntNumber("1"), ContextSource.MAP);
    }
}
