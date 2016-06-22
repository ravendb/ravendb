using System;

using Microsoft.CodeAnalysis.CSharp;

using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;

namespace Raven.Server.Documents.Indexes.Static.Roslyn
{
    internal class QuerySyntaxMapRewriter : MapRewriterBase
    {
        private readonly QuerySyntaxCollectionRewriter _collectionRewriter = new QuerySyntaxCollectionRewriter();

        public QuerySyntaxMapRewriter()
        {
            Rewriters = new CSharpSyntaxRewriter[]
            {
                _collectionRewriter,
                DynamicExtensionMethodsRewriter.Instance
            };
        }

        public override string CollectionName
        {
            get
            {
                return _collectionRewriter.CollectionName;
            }

            protected set
            {
                throw new NotSupportedException();
            }
        }
    }
}