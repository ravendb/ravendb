using System.Collections.Generic;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;

namespace Raven.Server.Documents.Indexes.Static.Roslyn
{
    public class MapFunctionProcessor : CSharpSyntaxRewriter
    {
        public readonly CSharpSyntaxRewriter CollectionRetriever;
        private readonly ReferencedCollectionsRetriever _refCollectionsRetriever;
        private readonly SelectManyRewriter _selectManyRewriter;

        public MapFunctionProcessor(CSharpSyntaxRewriter collectionRetriever, SelectManyRewriter selectManyRewriter)
        {
            _selectManyRewriter = selectManyRewriter;
            CollectionRetriever = collectionRetriever;
            _refCollectionsRetriever = new ReferencedCollectionsRetriever();
        }

        public HashSet<string> ReferencedCollections => _refCollectionsRetriever.ReferencedCollections;

        public override SyntaxNode Visit(SyntaxNode node)
        {
            foreach (var rewriter in new CSharpSyntaxRewriter[]
            {
                CollectionRetriever,
                _refCollectionsRetriever,
                _selectManyRewriter,
                SelectManyRewriter.SelectMethodOnProperties,
                new ThrowOnInvalidMethodCalls(),
                WhereRewriter.Instance,
                DynamicInvocationExpressionsRewriter.Instance,
                DynamicExtensionMethodsRewriter.Instance,
                DynamicLambdaExpressionsRewriter.Instance,
                RecurseRewriter.Instance,
                SpatialFieldRewriter.Instance,
                ConditionalAccessExpressionRewriter.Instance,
                CoalesceRewriter.Instance,
                InitializerExpressionRewriter.Instance,
                NullRewriter.Instance,
                IsRewriter.Instance
            })
            {
                node = rewriter.Visit(node);
            }

            return node;
        }
    }
}
