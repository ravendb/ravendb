using System;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;

namespace Raven.Server.Documents.Indexes.Static.Roslyn
{
    internal class MethodSyntaxMapRewriter : MapRewriter
    {
        private readonly MethodSyntaxCollectionRewriter _collectionRewriter = new MethodSyntaxCollectionRewriter();

        private readonly CSharpSyntaxRewriter[] _rewriters;

        public MethodSyntaxMapRewriter()
        {
            _rewriters = new CSharpSyntaxRewriter[]
            {
                _collectionRewriter,
                new SelectManyRewriter(),
                new DynamicExtensionMethodsRewriter()
            };
        }

        public override string CollectionName
        {
            get
            {
                return _collectionRewriter.CollectionName;
            }

            protected set
            {
                throw new NotSupportedException();
            }
        }

        public override SyntaxNode Visit(SyntaxNode node)
        {
            foreach (var rewriter in _rewriters)
                node = rewriter.Visit(node);

            return node;
        }
    }
}