using Microsoft.CodeAnalysis.CSharp;
using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;

namespace Raven.Server.Documents.Indexes.Static.Roslyn
{
    internal class MethodSyntaxTransformResultsRewriter : TransformResultsRewriterBase
    {
        public MethodSyntaxTransformResultsRewriter()
        {
            Rewriters = new CSharpSyntaxRewriter[]
            {
                SelectManyRewriter.MethodSyntax,
                DynamicExtensionMethodsRewriter.Instance,
                DynamicLambdaExpressionsRewriter.Instance,
                RecurseRewriter.Instance
            };
        }
    }
}