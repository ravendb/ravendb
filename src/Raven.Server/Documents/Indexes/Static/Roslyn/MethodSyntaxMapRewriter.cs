using System;

using Microsoft.CodeAnalysis.CSharp;

using Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;

namespace Raven.Server.Documents.Indexes.Static.Roslyn
{
    internal class MethodSyntaxMapRewriter : MapRewriterBase
    {
        private readonly MethodSyntaxCollectionRewriter _collectionRewriter = new MethodSyntaxCollectionRewriter();

        public MethodSyntaxMapRewriter()
        {
            Rewriters = new CSharpSyntaxRewriter[]
            {
                _collectionRewriter,
                SelectManyRewriter.Instance,
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