using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace Raven.Server.Documents.Indexes.Static.Roslyn
{
    public class CSharpSyntaxRewriterBase : CSharpSyntaxRewriter
    {
        protected CSharpSyntaxRewriter[] Rewriters;

        public override SyntaxNode Visit(SyntaxNode node)
        {
            foreach (var rewriter in Rewriters)
                node = rewriter.Visit(node);

            return node;
        }
    }
}