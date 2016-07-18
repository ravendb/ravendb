using System.Collections.Generic;

namespace Raven.Server.Documents.Indexes.Static.Roslyn
{
    public abstract class MapRewriterBase : CSharpSyntaxRewriterBase
    {
        public abstract string CollectionName { get; protected set; }

        public abstract HashSet<string> ReferencedCollections { get; protected set; }
    }
}