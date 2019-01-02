using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public class WhereRewriter : CSharpSyntaxRewriter
    {
        public static WhereRewriter Instance => new WhereRewriter();

        public override SyntaxNode VisitParenthesizedLambdaExpression(ParenthesizedLambdaExpressionSyntax node)
        {
            if (node.Parent == null)
                return base.VisitParenthesizedLambdaExpression(node);

            var argument = node.Parent as ArgumentSyntax;
            if (argument == null)
                return base.VisitParenthesizedLambdaExpression(node);

            var argumentList = argument.Parent as ArgumentListSyntax;
            if (argumentList == null)
                return base.VisitParenthesizedLambdaExpression(node);

            var invocation = argumentList.Parent as InvocationExpressionSyntax;
            if (invocation == null)
                return base.VisitParenthesizedLambdaExpression(node);

            var identifier = invocation.Expression
                .DescendantNodes(descendIntoChildren: syntaxNode => true)
                .LastOrDefault(x => x.IsKind(SyntaxKind.IdentifierName)) as IdentifierNameSyntax;

            if (identifier == null)
                return base.VisitParenthesizedLambdaExpression(node);

            return HandleMethod(node, invocation, identifier.Identifier.Text);
        }

        public override SyntaxNode VisitSimpleLambdaExpression(SimpleLambdaExpressionSyntax node)
        {
            if (node.Parent == null)
                return base.VisitSimpleLambdaExpression(node);

            var argument = node.Parent as ArgumentSyntax;
            if (argument == null)
                return base.VisitSimpleLambdaExpression(node);

            var argumentList = argument.Parent as ArgumentListSyntax;
            if (argumentList == null)
                return base.VisitSimpleLambdaExpression(node);

            var invocation = argumentList.Parent as InvocationExpressionSyntax;
            if (invocation == null)
                return base.VisitSimpleLambdaExpression(node);

            var identifier = invocation.Expression
                .DescendantNodes(descendIntoChildren: syntaxNode => true)
                .LastOrDefault(x => x.IsKind(SyntaxKind.IdentifierName)) as IdentifierNameSyntax;

            if (identifier == null)
                return base.VisitSimpleLambdaExpression(node);

            return HandleMethod(node, invocation, identifier.Identifier.Text);
        }

        public override SyntaxNode VisitWhereClause(WhereClauseSyntax node)
        {
            var condition = HandleCondition(node.Condition);
            return node.WithCondition(condition);
        }

        private static ExpressionSyntax HandleCondition(ExpressionSyntax expression)
        {
            if (expression is ParenthesizedExpressionSyntax pes)
                return pes.WithExpression(HandleCondition(pes.Expression));

            if (expression is BinaryExpressionSyntax bes && (bes.IsKind(SyntaxKind.LogicalAndExpression) || bes.IsKind(SyntaxKind.LogicalOrExpression)))
            {
                return bes
                    .WithLeft(SyntaxFactory.ParseExpression($"(bool)({HandleCondition(bes.Left)})"))
                    .WithRight(SyntaxFactory.ParseExpression($"(bool)({HandleCondition(bes.Right)})"));
            }

            return expression;
        }

        private static SyntaxNode HandleMethod(LambdaExpressionSyntax node, InvocationExpressionSyntax invocation, string method)
        {
            switch (method)
            {
                case "Any":
                case "All":
                case "First":
                case "FirstOrDefault":
                case "Last":
                case "LastOfDefault":
                case "Single":
                case "Where":
                case "Count":
                case "SingleOrDefault":
                    return ModifyLambdaForBools(node);
            }

            return node;
        }

        private static SyntaxNode ModifyLambdaForBools(LambdaExpressionSyntax node)
        {
            if (node.Body is ExpressionSyntax expression)
                return node.WithBody(HandleCondition(expression));

            return node;
        }
    }
}
