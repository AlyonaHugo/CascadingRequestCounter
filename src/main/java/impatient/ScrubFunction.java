package impatient;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


public class ScrubFunction extends BaseOperation implements Function
{
    public ScrubFunction( Fields fieldDeclaration )
    {
        super( 2, fieldDeclaration );
    }

    public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
        TupleEntry argument = functionCall.getArguments();
        System.out.println("argument " + argument);
        String doc_id = getDataAroungHour(argument.getString( 0 ));
        System.out.println("doc_id " + doc_id);
        String token = scrubText( argument.getString( 1 ) );
        System.out.println("token " + token);

        if( token != null )
        {
            Tuple result = new Tuple();
            result.add( doc_id );
            result.add( token );
            functionCall.getOutputCollector().add( result );
        }
    }

    public String scrubText( String text ) {
        return text.substring(1, text.length());
    }

    public String getDataAroungHour( String text ) {
        int numOfSymbol = 3;// 3 -hours, -2-minutes
        return text.substring(1, text.lastIndexOf(":") - numOfSymbol);
    }
}
