using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;

namespace Raven.Server.Documents.Indexes.Static.Roslyn
{
    public class TransformFunctionProcessor : CSharpSyntaxRewriter
    {
        private SelectManyRewriter _selectManyRewriter;

        public TransformFunctionProcessor(SelectManyRewriter selectManyRewriter)
        {
            _selectManyRewriter = selectManyRewriter;
        }

        public override SyntaxNode Visit(SyntaxNode node)
        {
            foreach (var rewriter in new CSharpSyntaxRewriter[]
            {
                _selectManyRewriter,
                DynamicInvocationExpressionsRewriter.Instance,
                DynamicExtensionMethodsRewriter.Instance,
                DynamicLambdaExpressionsRewriter.Instance,
                RecurseRewriter.Instance,
            })
            {
                node = rewriter.Visit(node);
            }

            return node;
        }
    }
}
