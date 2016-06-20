using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters.ReduceIndex
{
    public abstract class ReduceRewriterBase : CSharpSyntaxRewriter
    {
        public abstract string ResultsVariableName { get; }

        protected CSharpSyntaxRewriter[] Rewriters;

        public override SyntaxNode Visit(SyntaxNode node)
        {
            foreach (var rewriter in Rewriters)
                node = rewriter.Visit(node);

            return node;
        }
    }
}