using System;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    internal class MethodSyntaxCollectionRewriter : CSharpSyntaxRewriter
    {
        public string CollectionName;

        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            if (CollectionName != null)
                return node;

            var nodeAsString = node.Expression.ToString();
            if (nodeAsString.StartsWith("docs") == false)
                return node;

            var nodeParts = nodeAsString.Split(new[] {"."}, StringSplitOptions.RemoveEmptyEntries);
            if (nodeParts.Length <= 2)
                return node;

            CollectionName = nodeParts[1];

            var collectionIndex = nodeAsString.IndexOf(CollectionName, StringComparison.OrdinalIgnoreCase);
            nodeAsString = nodeAsString.Remove(collectionIndex - 1, CollectionName.Length + 1); // removing .Users

            var newExpression = SyntaxFactory.ParseExpression(nodeAsString);
            return node.WithExpression(newExpression);
        }
    }
}