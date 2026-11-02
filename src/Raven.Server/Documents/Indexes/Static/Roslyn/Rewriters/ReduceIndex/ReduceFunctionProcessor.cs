using System.Collections.Generic;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters.ReduceIndex
{
    public sealed class ReduceFunctionProcessor : CSharpSyntaxRewriter
    {
        private readonly ResultsVariableNameRewriter _setResultsVariable;
        private readonly GroupByFieldsRetriever _getGroupByFields;
        private readonly SelectManyRewriter _selectManyRewriter;
        private readonly string _dynamicEnumerable;

        public ReduceFunctionProcessor(ResultsVariableNameRewriter setResultsVariable, GroupByFieldsRetriever getGroupByFields, SelectManyRewriter selectManyRewriter, string dynamicEnumerable)
        {
            _setResultsVariable = setResultsVariable;
            _getGroupByFields = getGroupByFields;
            _selectManyRewriter = selectManyRewriter;
            _dynamicEnumerable = dynamicEnumerable;
        }

        public CompiledIndexField[] GroupByFields => _getGroupByFields.GroupByFields;

        public override SyntaxNode Visit(SyntaxNode node)
        {
            var rangeSourceParameters = new HashSet<string>();
            foreach (var rewriter in new CSharpSyntaxRewriter[]
            {
                _setResultsVariable,
                _getGroupByFields,
                WhereRewriter.Instance,
                new DynamicInvocationExpressionsRewriter(rangeSourceParameters, _dynamicEnumerable),
                new DynamicLambdaExpressionsRewriter(rangeSourceParameters, _dynamicEnumerable),
                _selectManyRewriter,
                SelectManyRewriter.SelectMethodOnProperties,
                SpatialFieldRewriter.Instance,
                ConditionalAccessExpressionRewriter.Instance,               
                CoalesceRewriter.Instance,
                InitializerExpressionRewriter.Instance,
                NullRewriter.Instance,
                IsRewriter.Instance,
                NoTrackingRewriter.Instance
            })
            {
                node = rewriter.Visit(node);
            }

            return node;
        }
    }
}
