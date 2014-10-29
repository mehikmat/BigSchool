package com.bigschool;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.fluid.api.assembly.Assembly.AssemblyBuilder;
import cascading.operation.Function;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.OuterJoin;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import org.junit.Test;

import static cascading.flow.FlowDef.flowDef;
import static cascading.fluid.Fluid.*;
import static cascading.pipe.Pipe.pipes;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class SimpleAssembliesTest
{
    @Test
    public void testSimpleGroup() throws Exception
    {
        Tap source = new FileTap( new TextLine( new Fields( "offset", "line" ) ), "data/input.txt" );
        Tap sink = new FileTap( new TextLine( new Fields( "offset", "line" ) ), "data/group_by.txt", SinkMode.REPLACE );
        AssemblyBuilder.Start assembly = assembly();
        Pipe pipe = assembly
                .startBranch( "test" )
                .each( fields( "line" ) )
                .function(
                        new WordSplitFunction()
                )
                .outgoing(fields( "ip" ))
                .each(fields( "ip" ))
                .filter(
                        filter().Debug().prefix(">").end()
                )
                .groupBy(fields("ip"))
                .every(Fields.ALL)
                .aggregator(
                        aggregator().
                                Count(fields("count"))
                )
                .outgoing(fields("ip", "count"))
                .completeGroupBy()
                .completeBranch();

        Flow flow = new LocalFlowConnector().connect( source, sink, pipe );
        flow.complete();
    }

    @Test
    public void testGroupByMerge()
    {
        Tap sourceLower = new FileTap( new TextLine( new Fields( "offset", "line" ) ), "data/input.txt" );
        Tap sourceUpper = new FileTap( new TextLine( new Fields( "offset", "line" ) ), "data/input.txt" );
        Tap sink = new FileTap( new TextLine( new Fields( "offset", "line" ) ), "data/group_by_merge.txt", SinkMode.REPLACE );

        Function splitter = function()
                .RegexSplitter()
                .fieldDeclaration( fields( "num", "char" ) )
                .patternString( " " )
                .end();
        AssemblyBuilder.Start assembly = assembly();
        Pipe pipeLower = assembly
                .startBranch( "lower" )
                .each( fields( "line" ) ).function( splitter ).outgoing( Fields.RESULTS )
                .completeBranch();
        Pipe pipeUpper = assembly
                .startBranch( "upper" )
                .each( fields( "line" ) ).function( splitter ).outgoing( Fields.RESULTS )
                .completeBranch();
        GroupBy merge = assembly
                .startGroupByMerge()
                .pipes( pipes( pipeLower, pipeUpper) )
                .groupFields( fields( "num" ) )
                .sortFields( fields( "char" ) )
                .createGroupByMerge();
        Pipe splice = assembly
                .continueBranch( merge )
                .every( fields( "char" ) )
                .aggregator(
                        aggregator().First().fieldDeclaration( fields( "first" ) ).end()
                )
                .outgoing( Fields.ALL )
                .each( fields( "num", "first" ) )
                .function(
                        function().Identity().fieldDeclaration( Fields.ALL ).end()
                )
                .outgoing( Fields.RESULTS )
                .completeBranch();
        FlowDef flowDef = flowDef()
                .addSource( "lower", sourceLower )
                .addSource( "upper", sourceUpper )
                .addTailSink( splice, sink );

        Flow flow = new LocalFlowConnector().connect( flowDef );
        flow.complete();
    }

    @Test
    public void testCoGroup() throws Exception
    {
        Tap sourceLower = new FileTap( new TextLine( new Fields( "offset", "line" ) ), "some/lower" );
        Tap sourceUpper = new FileTap( new TextLine( new Fields( "offset", "line" ) ), "some/upper" );
        Tap sink = new FileTap( new TextLine( new Fields( "line" ) ), "some/result", SinkMode.REPLACE );

       // Factories for all Operations (Functions, Filters, Aggregators, and Buffers)
        Function splitter = function()
                .RegexSplitter()
                .fieldDeclaration( fields( "num", "char" ) )
                .patternString( " " )
                .end();
       // An assembly builder chaining Pipes into complex assemblies
        AssemblyBuilder.Start assembly = assembly();
        Pipe pipeLower = assembly
                .startBranch( "lower" )
                .each( fields( "line" ) ).function( splitter ).outgoing( Fields.RESULTS )
                .completeBranch();
        Pipe pipeUpper = assembly
                .startBranch( "upper" )
                .each( fields( "line" ) ).function( splitter ).outgoing( Fields.RESULTS )
                .completeBranch();
       // todo: let lhs/rhs take names and resolve them in the current assembly
        Pipe coGroup = assembly
                .startCoGroup()
                .lhs( pipeLower ).lhsGroupFields( fields( "num" ) )
                .rhs( pipeUpper ).rhsGroupFields( fields( "num" ) )
                .declaredFields( fields( "num1", "char1", "num2", "char2" ) )
                .joiner( new OuterJoin() )
                .createCoGroup();
        assembly
                .continueBranch( "result", coGroup )
                .retain( fields( "num1", "char1" ) )
                .rename( Fields.ALL, fields( "num", "char" ) )
                .completeBranch();
        Pipe[] tails = assembly.completeAssembly();
        FlowDef flowDef = flowDef()
                .addSource( "lower", sourceLower )
                .addSource( "upper", sourceUpper )
                .addSink( "result", sink )
                .addTails( tails );

        Flow flow = new LocalFlowConnector().connect( flowDef );
        flow.complete();
    }

    @Test
    public void testHashJoin() throws Exception
    {
        Tap sourceLower = new FileTap( new TextLine( new Fields( "offset", "line" ) ), "some/lower" );
        Tap sourceUpper = new FileTap( new TextLine( new Fields( "offset", "line" ) ), "some/upper" );
        Tap sourceLhs = new FileTap( new TextLine( new Fields( "offset", "line" ) ), "some/lhs" );
        Tap sourceRhs = new FileTap( new TextLine( new Fields( "offset", "line" ) ), "some/rhs" );
        Tap sink = new FileTap( new TextLine( new Fields( "line" ) ), "some/result", SinkMode.REPLACE );

        // Factories for all Operations (Functions, Filters, Aggregators, and Buffers)
        Function splitter = function()
                .RegexSplitter()
                .fieldDeclaration( fields( "num", "char" ) )
                .patternString( " " )
                .end();
        // An assembly builder chaining Pipes into complex assemblies
        AssemblyBuilder.Start assembly = assembly();
        Pipe pipeLower = assembly
                .startBranch( "lower" )
                .each( fields( "line" ) ).function( splitter ).outgoing( Fields.RESULTS )
                .completeBranch();
        Pipe pipeUpper = assembly
                .startBranch( "upper" )
                .each( fields( "line" ) ).function( splitter ).outgoing( Fields.RESULTS )
                .completeBranch();
        Pipe pipeLhs = assembly
                .startBranch( "lhs" )
                .each( fields( "line" ) ).function( splitter ).outgoing( Fields.RESULTS )
                .completeBranch();
        Pipe pipeRhs = assembly
                .startBranch( "rhs" )
                .each( fields( "line" ) ).function( splitter ).outgoing( Fields.RESULTS )
                .completeBranch();
        Pipe upperLower = assembly
                .startHashJoin()
                .lhs( pipeLower ).lhsJoinFields( fields( "num" ) )
                .rhs( pipeUpper ).rhsJoinFields( fields( "num" ) )
                .declaredFields( fields( "numUpperLower", "charUpperLower", "num2UpperLower", "char2UpperLower" ) )
                .createHashJoin();
        upperLower = assembly
                .continueBranch( upperLower )
                .each( Fields.ALL ).function
                        (
                                function().Identity().fieldDeclaration( Fields.ALL ).end()
                        )
                .outgoing( Fields.RESULTS )
                .completeBranch();
        Pipe lhsUpperLower = assembly
                .startHashJoin()
                .lhs( pipeLhs ).lhsJoinFields( fields( "num" ) )
                .rhs( upperLower ).rhsJoinFields( fields( "numUpperLower" ) )
                .declaredFields( fields( "numLhs", "charLhs", "numUpperLower", "charUpperLower", "num2UpperLower", "char2UpperLower" ) )
                .createHashJoin();
        lhsUpperLower = assembly
                .continueBranch( lhsUpperLower )
                .each( Fields.ALL ).function
                        (
                                function().Identity().fieldDeclaration( Fields.ALL ).end()
                        )
                .outgoing( Fields.RESULTS )
                .completeBranch();
        assembly
                .continueBranch
                        (
                                assembly
                                        .startCoGroup()
                                        .groupName( "cogrouping" )
                                        .lhs( lhsUpperLower ).lhsGroupFields( fields( "numLhs" ) )
                                        .rhs( pipeRhs ).rhsGroupFields( fields( "num" ) )
                                        .createCoGroup()
                        )
                .completeBranch();
        Pipe[] tails = assembly.completeAssembly();
        FlowDef flowDef = flowDef()
                .addSource( "lower", sourceLower )
                .addSource( "upper", sourceUpper )
                .addSource( "lhs", sourceLhs )
                .addSource( "rhs", sourceRhs )
                .addSink( "cogrouping", sink )
                .addTails( tails );

        Flow flow = new LocalFlowConnector().connect( flowDef );
        flow.complete();
    }
}
