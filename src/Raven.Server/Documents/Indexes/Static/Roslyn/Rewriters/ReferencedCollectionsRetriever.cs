using System;
using System.Collections.Generic;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    internal sealed class ReferencedCollectionsRetriever : CSharpSyntaxRewriter
    {
        public HashSet<string> ReferencedCollections;

        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            var loadDocument = node.Expression.ToString();
            
            if (loadDocument is "this.CreateVector" or "CreateVector")
                CreateReferencedCollections();
            
            if (loadDocument != "this.LoadDocument" && loadDocument != "LoadDocument")
                return base.VisitInvocationExpression(node);

            if (node.ArgumentList.Arguments.Count <= 1)
                return base.VisitInvocationExpression(node);

            if (node.ArgumentList.Arguments[^1].Expression is not LiteralExpressionSyntax collectionLiteral)
                return base.VisitInvocationExpression(node);

            CreateReferencedCollections();
            ReferencedCollections.Add(collectionLiteral.Token.Value.ToString());

            return base.VisitInvocationExpression(node);
        }
        
        private void CreateReferencedCollections()
        {
            ReferencedCollections ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }
    }
}
