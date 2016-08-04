using System.Linq;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public class DynamicLambdaExpressionsRewriter : CSharpSyntaxRewriter
    {
        public static DynamicLambdaExpressionsRewriter Instance = new DynamicLambdaExpressionsRewriter();

        private DynamicLambdaExpressionsRewriter()
        {
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

            var method = identifier.Identifier.Text;

            switch (method)
            {
                case "Select":
                    return Visit(ModifyLambdaForSelect(node, invocation));
                case "SelectMany":
                    return Visit(ModifyLambdaForSelectMany(node));
                case "Sum":
                case "Average":
                    return Visit(ModifyLambdaForNumerics(node));
            }

            return base.VisitSimpleLambdaExpression(node);
        }

        private static SyntaxNode ModifyLambdaForSelectMany(SimpleLambdaExpressionSyntax node)
        {
            return SyntaxFactory.ParseExpression($"(Func<dynamic, IEnumerable<dynamic>>)({node})");
        }

        private static SyntaxNode ModifyLambdaForSelect(SimpleLambdaExpressionSyntax node, InvocationExpressionSyntax currentInvocation)
        {
            var parentMethod = GetParentMethod(currentInvocation);
            switch (parentMethod)
            {
                case "GroupBy":
                    return SyntaxFactory.ParseExpression($"(Func<IGrouping<dynamic, dynamic>, dynamic>)({node})");
                default:
                    return SyntaxFactory.ParseExpression($"(Func<dynamic, dynamic>)({node})");
            }
        }

        private static string GetParentMethod(InvocationExpressionSyntax currentInvocation)
        {
            var invocation = currentInvocation.Expression
                .DescendantNodes(descendIntoChildren: syntaxNode => true)
                .FirstOrDefault(x => x.IsKind(SyntaxKind.InvocationExpression)) as InvocationExpressionSyntax;

            var member = invocation?.Expression as MemberAccessExpressionSyntax;
            return member?.Name.Identifier.Text;
        }

        private static SyntaxNode ModifyLambdaForNumerics(SimpleLambdaExpressionSyntax node)
        {
            var alreadyCasted = node.Body as CastExpressionSyntax;

            if (alreadyCasted != null)
            {
                return SyntaxFactory.ParseExpression($"(Func<dynamic, {alreadyCasted.Type}>)({node})");
            }

            var cast = (CastExpressionSyntax)SyntaxFactory.ParseExpression($"(decimal){node.Body}");

            return SyntaxFactory.ParseExpression($"(Func<dynamic, decimal>)({node.WithBody(cast)})");
        }
    }
}