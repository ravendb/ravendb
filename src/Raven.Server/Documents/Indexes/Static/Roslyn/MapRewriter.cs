using Microsoft.CodeAnalysis.CSharp;

namespace Raven.Server.Documents.Indexes.Static.Roslyn
{
    public abstract class MapRewriter : CSharpSyntaxRewriter
    {
        public abstract string CollectionName { get; protected set; }
    }
}