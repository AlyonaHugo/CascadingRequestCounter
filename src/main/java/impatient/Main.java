
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.operation.aggregator.Count;
import cascading.pipe.*;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import impatient.ScrubFunction;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Properties;


public class Main {
    public static void main(String[] args) {
        String docPath = args[0];
        String wcPath = args[1];

        //  cleanDirectory(wcPath);

        FlowConnector flowConnector = createFlowConnector();

        // create source and sink taps
        Tap wcTap = new Hfs(new TextDelimited(true, " "), wcPath);

        Fields allFiles = new Fields("ip", "res", "res1", "data", "sec", "crud", "url", "http", "status", "status1", "site", "response");
        Tap allTap = new Hfs(new TextDelimited(allFiles, true, " "), docPath);


        Pipe allfilePipe = new Pipe("allfilePipe");

        Pipe outPipe = leftchosenFiels(allfilePipe);
        Pipe wcPipe = summarize(outPipe);


        // build flow definition
        FlowDef flowDef = FlowDef.flowDef().setName("myFlow")
                .addSource(allfilePipe, allTap)
                .addTailSink(wcPipe, wcTap);


        // write a DOT file and run the flow
        Flow wcFlow = flowConnector.connect(flowDef);
        wcFlow.writeDOT("dot/wc.dot");
        wcFlow.complete();
    }

    private static void cleanDirectory(String wcPath) {
        try {
            File f = new File(wcPath);
            FileUtils.cleanDirectory(f); //clean out directory (this is optional -- but good know)
            FileUtils.forceDelete(f); //delete directory
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static FlowConnector createFlowConnector() {
        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, Main.class);
        AppProps.setApplicationName(properties, "Part 1");
        AppProps.addApplicationTag(properties, "lets:do:it");
        AppProps.addApplicationTag(properties, "technology:Cascading");
        return new Hadoop2MR1FlowConnector(properties);
    }

    private static Pipe leftchosenFiels(Pipe allfilePipe) {

        Pipe outPipe = new Pipe("source",allfilePipe);
        // define "ScrubFunction" to clean up the token stream
        Fields scrubArguments = new Fields("data", "crud");
        outPipe = new Each(outPipe, scrubArguments, new ScrubFunction(scrubArguments), Fields.RESULTS);
        return outPipe;
    }

    private static Pipe summarize(Pipe outPipe) {
        Fields channelFields = new Fields("data", "crud");
        Fields amount = new Fields("crud");
        Pipe wcPipe = new Pipe("wc", outPipe);
        wcPipe = new GroupBy(wcPipe, channelFields);
        wcPipe = new Every(wcPipe, amount, new Count(), Fields.ALL);
        return wcPipe;
    }
}