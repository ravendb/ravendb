using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters.ReduceIndex
{
    public class ReduceFunctionProcessor : CSharpSyntaxRewriter
    {
        private readonly ResultsVariableNameRewriter _setResultsVariable;
        private readonly GroupByFieldsRetriever _getGroupByFields;
        private readonly SelectManyRewriter _selectManyRewriter;

        public ReduceFunctionProcessor(ResultsVariableNameRewriter setResultsVariable, GroupByFieldsRetriever getGroupByFields, SelectManyRewriter selectManyRewriter)
        {
            _setResultsVariable = setResultsVariable;
            _getGroupByFields = getGroupByFields;
            _selectManyRewriter = selectManyRewriter;
        }

        public Field[] GroupByFields => _getGroupByFields.GroupByFields;

        public override SyntaxNode Visit(SyntaxNode node)
        {
            foreach (var rewriter in new CSharpSyntaxRewriter[]
            {
                _setResultsVariable,
                _getGroupByFields,
                DynamicLambdaExpressionsRewriter.Instance,
                _selectManyRewriter,
                SelectManyRewriter.SelectMethodOnProperties,
                SpatialFieldRewriter.Instance,
                CoalesceRewriter.Instance,
                InitializerExpressionRewriter.Instance,
                NullRewriter.Instance
            })
            {
                node = rewriter.Visit(node);
            }

            return node;
        }
    }
}
