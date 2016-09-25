using System.Collections.Generic;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;

namespace Raven.Server.Documents.Indexes.Static.Roslyn
{
    public class MapFunctionProcessor : CSharpSyntaxRewriter
    {
        private readonly CollectionNameRetriever _collectionRetriever;
        private readonly ReferencedCollectionsRetriever _refCollectionsRetriever;
        private SelectManyRewriter _selectManyRewriter;

        public MapFunctionProcessor(CollectionNameRetriever collectionRetriever, SelectManyRewriter selectManyRewriter)
        {
            _selectManyRewriter = selectManyRewriter;
            _collectionRetriever = collectionRetriever;
            _refCollectionsRetriever = new ReferencedCollectionsRetriever();
        }

        public string CollectionName => _collectionRetriever.CollectionName;

        public HashSet<string> ReferencedCollections => _refCollectionsRetriever.ReferencedCollections;

        public override SyntaxNode Visit(SyntaxNode node)
        {
            foreach (var rewriter in new CSharpSyntaxRewriter[]
            {
                _collectionRetriever,
                _refCollectionsRetriever,
                _selectManyRewriter,
                DynamicExtensionMethodsRewriter.Instance,
                DynamicLambdaExpressionsRewriter.Instance,
                RecurseRewriter.Instance
            })
            {
                node = rewriter.Visit(node);
            }

            return node;
        }
    }
}
