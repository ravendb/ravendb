using System.Collections.Generic;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace Raven.Server.Documents.Indexes.Static.Roslyn
{
    public abstract class MapRewriterBase : CSharpSyntaxRewriter
    {
        public abstract string CollectionName { get; protected set; }

        public abstract HashSet<string> ReferencedCollections { get; protected set; }

        protected CSharpSyntaxRewriter[] Rewriters;

        public override SyntaxNode Visit(SyntaxNode node)
        {
            foreach (var rewriter in Rewriters)
                node = rewriter.Visit(node);

            return node;
        }
    }
}