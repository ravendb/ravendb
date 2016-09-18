using Microsoft.CodeAnalysis.CSharp;
using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;

namespace Raven.Server.Documents.Indexes.Static.Roslyn
{
    internal class QuerySyntaxTransformResultsRewriter : TransformResultsRewriterBase
    {
        private readonly MethodDetectorRewriter _methods;

        public QuerySyntaxTransformResultsRewriter()
        {
            _methods = new MethodDetectorRewriter();
            Rewriters = new CSharpSyntaxRewriter[]
            {
                DynamicExtensionMethodsRewriter.Instance,
                DynamicLambdaExpressionsRewriter.Instance,
                RecurseRewriter.Instance,
                _methods
            };
        }

        public override IndexAndTransformerCompiler.IndexAndTransformerMethods Methods => _methods.Methods;
    }
}