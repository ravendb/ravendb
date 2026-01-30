using System;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public sealed class SchemaValidatorRewriter : CSharpSyntaxRewriter
    {
        public static readonly SchemaValidatorRewriter Instance = new SchemaValidatorRewriter();

        private SchemaValidatorRewriter()
        {
        }

        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            var expression = node.Expression.ToString();
            if (TryRemoveSchema(expression, out var newExpression) == false)
                return base.VisitInvocationExpression(node);

            var newNode = SyntaxFactory.ParseExpression(node.ToString().Replace(expression, newExpression));
            return Visit(newNode);
        }

        private static bool TryRemoveSchema(string expression, out string result)
        {
            const string thisRef = "this.";
            const string toStrip = "Schema.GetErrorsFor";
            const string generatedMethodName = "SchemaGetErrorsFor";
            
            ReadOnlySpan<char> span = expression.AsSpan(expression.StartsWith(thisRef) ? thisRef.Length : 0);
            if (span.StartsWith(toStrip))
            {
                result = expression.Replace(toStrip, generatedMethodName);
                return true;
            }
            
            result = null;
            return false;
        }
    }
}
