using Microsoft.CodeAnalysis.CSharp;
using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;

namespace Raven.Server.Documents.Indexes.Static.Roslyn
{
    internal class QuerySyntaxTransformResultsRewriter : TransformResultsRewriterBase
    {
        public QuerySyntaxTransformResultsRewriter()
        {
            Rewriters = new CSharpSyntaxRewriter[]
            {
                DynamicExtensionMethodsRewriter.Instance,
                RecurseRewriter.Instance
            };
        }
    }
}