using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public class RecurseRewriter : CSharpSyntaxRewriter
    {
        public static RecurseRewriter Instance = new RecurseRewriter();

        private RecurseRewriter()
        {
        }

        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            var recurse = node.Expression.ToString();
            if (recurse != "this.Recurse" && recurse != "Recurse")
                return base.VisitInvocationExpression(node);

            if (node.ArgumentList.Arguments.Count <= 1)
                return base.VisitInvocationExpression(node);

            var argument = node.ArgumentList.Arguments[1];
            var func = argument.Expression as LambdaExpressionSyntax;
            if (func == null)
                return base.VisitInvocationExpression(node);

            var toCast = func;
            var cast = SyntaxFactory.ParseExpression($"(Func<dynamic, dynamic>)({toCast})");

            var argumentList = node
                .ArgumentList
                .WithArguments(node.ArgumentList.Arguments.Replace(argument, argument.WithExpression(cast)));

            return node.WithArgumentList(argumentList);
        }
    }
}