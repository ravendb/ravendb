using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters.ReduceIndex
{
    public class ReduceFunctionProcessor : CSharpSyntaxRewriter
    {
        private readonly ResultsVariableNameRetriever _resultsVariable;
        private readonly GroupByFieldsRetriever _groupByFields;
        private readonly SelectManyRewriter _selectManyRewriter;

        public ReduceFunctionProcessor(ResultsVariableNameRetriever resultsVariable, GroupByFieldsRetriever groupByFields, SelectManyRewriter selectManyRewriter)
        {
            _resultsVariable = resultsVariable;
            _groupByFields = groupByFields;
            _selectManyRewriter = selectManyRewriter;
        }

        public string ResultsVariableName => _resultsVariable.ResultsVariableName;

        public string[] GroupByFields => _groupByFields.GroupByFields;

        public override SyntaxNode Visit(SyntaxNode node)
        {
            foreach (var rewriter in new CSharpSyntaxRewriter[]
            {
                _resultsVariable,
                _groupByFields,
                DynamicLambdaExpressionsRewriter.Instance,
                _selectManyRewriter
            })
            {
                if (rewriter == _groupByFields)
                    _groupByFields.Initialize(_resultsVariable.ResultsVariableName);

                node = rewriter.Visit(node);
            }

            return node;
        }
    }
}