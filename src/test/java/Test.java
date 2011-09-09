import com.mapreducelite.MRJob;
import com.mapreducelite.JobSubmitor;
import com.mapreducelite.type.IntNumber;
import com.mapreducelite.type.MRText;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: yibing.tan
 * Date: 11-8-24
 * Time: 上午11:02
 * To change this template use File | Settings | File Templates.
 */
public class Test {

    public static void main(String[] args) throws Exception {
        JobSubmitor.submit(createJob());
    }

    private static MRJob createJob() throws Exception{
        String test = "cat hello test\ncat world hello";
        List<InputStream> ins = new LinkedList<InputStream>();
        byte[] bytes = test.getBytes("utf-8");
        InputStream is = new ByteArrayInputStream(bytes);
        ins.add(is);
         MRJob job1 = new MRJob("test-mr",ins);

        job1.setKeyClass(MRText.class);
        job1.setValueClass(IntNumber.class);

        job1.setMapperClass(TestMapper.class);
        job1.setReducerClass(TestReducer.class);
        return job1;
    }
}
