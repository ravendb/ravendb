using System;
using System.Collections.Generic;
using Microsoft.CodeAnalysis.CSharp;

using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;

namespace Raven.Server.Documents.Indexes.Static.Roslyn
{
    internal class MethodSyntaxMapRewriter : MapRewriterBase
    {
        private readonly MethodSyntaxCollectionRewriter _collectionRewriter = new MethodSyntaxCollectionRewriter();

        private readonly ReferencedCollectionRewriter _referencedCollectionRewriter = new ReferencedCollectionRewriter();

        public MethodSyntaxMapRewriter()
        {
            Rewriters = new CSharpSyntaxRewriter[]
            {
                _collectionRewriter,
                _referencedCollectionRewriter,
                SelectManyRewriter.Instance,
                DynamicExtensionMethodsRewriter.Instance,
                RecurseRewriter.Instance
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