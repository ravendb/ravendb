using System;
using System.Collections.Generic;
using Microsoft.CodeAnalysis.CSharp;

using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;

namespace Raven.Server.Documents.Indexes.Static.Roslyn
{
    internal class QuerySyntaxMapRewriter : MapRewriterBase
    {
        private readonly QuerySyntaxCollectionRewriter _collectionRewriter = new QuerySyntaxCollectionRewriter();

        private readonly ReferencedCollectionRewriter _referencedCollectionRewriter = new ReferencedCollectionRewriter();

        public QuerySyntaxMapRewriter()
        {
            Rewriters = new CSharpSyntaxRewriter[]
            {
                _collectionRewriter,
                _referencedCollectionRewriter,
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

        public override HashSet<string> ReferencedCollections
        {
            get
            {
                return _referencedCollectionRewriter.ReferencedCollections;
            }

            protected set
            {
                throw new NotSupportedException();
            }
        }
    }
}