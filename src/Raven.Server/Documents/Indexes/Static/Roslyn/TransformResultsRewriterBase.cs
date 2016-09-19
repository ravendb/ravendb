namespace Raven.Server.Documents.Indexes.Static.Roslyn
{
    internal abstract class TransformResultsRewriterBase : CSharpSyntaxRewriterBase
    {
        public abstract IndexAndTransformerCompiler.IndexAndTransformerMethods Methods { get; }
    }
}