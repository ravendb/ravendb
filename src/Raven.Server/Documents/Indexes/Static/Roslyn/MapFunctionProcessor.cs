using System.Collections.Generic;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;

namespace Raven.Server.Documents.Indexes.Static.Roslyn
{
    public sealed class MapFunctionProcessor : CSharpSyntaxRewriter
    {
        public readonly CSharpSyntaxRewriter CollectionRetriever;
        private readonly ReferencedCollectionsRetriever _refCollectionsRetriever;
        private readonly CompareExchangeReferenceDetectorRewriter _compareExchangeReferenceDetector;
        private readonly SelectManyRewriter _selectManyRewriter;
        private readonly string _dynamicEnumerable;

        public MapFunctionProcessor(CSharpSyntaxRewriter collectionRetriever, SelectManyRewriter selectManyRewriter, string dynamicEnumerable)
        {
            _selectManyRewriter = selectManyRewriter;
            _dynamicEnumerable = dynamicEnumerable;
            CollectionRetriever = collectionRetriever;
            _refCollectionsRetriever = new ReferencedCollectionsRetriever();
            _compareExchangeReferenceDetector = new CompareExchangeReferenceDetectorRewriter();
        }

        public HashSet<string> ReferencedCollections => _refCollectionsRetriever.ReferencedCollections;

        public bool HasLoadCompareExchangeValue => _compareExchangeReferenceDetector.HasLoadCompareExchangeValue;

        public override SyntaxNode Visit(SyntaxNode node)
        {
            var rangeSourceParameters = new HashSet<string>();
            foreach (var rewriter in new CSharpSyntaxRewriter[]
            {
                CollectionRetriever,
                _refCollectionsRetriever,
                _compareExchangeReferenceDetector,
                _selectManyRewriter,
                SelectManyRewriter.SelectMethodOnProperties,
                new ThrowOnInvalidMethodCalls(),
                WhereRewriter.Instance,
                new DynamicInvocationExpressionsRewriter(rangeSourceParameters, _dynamicEnumerable),
                DynamicExtensionMethodsRewriter.Instance,
                new DynamicLambdaExpressionsRewriter(rangeSourceParameters, _dynamicEnumerable),
                RecurseRewriter.Instance,
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
