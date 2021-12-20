using System;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public class NoTrackingRewriter : CSharpSyntaxRewriter
    {
        public static readonly NoTrackingRewriter Instance = new NoTrackingRewriter();

        private NoTrackingRewriter()
        {
        }

        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            var expression = node.Expression.ToString();
            if (TryRemoveNoTracking(expression, out var newExpression) == false)
                return base.VisitInvocationExpression(node);

            var newNode = SyntaxFactory.ParseExpression(node.ToString().Replace(expression, newExpression));
            return base.Visit(newNode);
        }

        public static bool TryRemoveNoTracking(string expression, out string result)
        {
            var toStrip = $"this.{nameof(AbstractCommonApiForIndexes.NoTracking)}.";
            if (TryStrip(expression, toStrip, out result))
                return true;

            toStrip = $"{nameof(AbstractCommonApiForIndexes.NoTracking)}.";
            if (TryStrip(expression, toStrip, out result))
                return true;

            result = null;
            return false;

            static bool TryStrip(string expression, string toStrip, out string result)
            {
                if (expression.StartsWith(toStrip))
                {
                    result = expression.Substring(toStrip.Length);
                    return true;
                }

                result = null;
                return false;
            }
        }
    }
}
