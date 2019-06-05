using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public sealed class DynamicInvocationExpressionsRewriter : CSharpSyntaxRewriter
    {
        public static readonly DynamicInvocationExpressionsRewriter Instance = new DynamicInvocationExpressionsRewriter();

        private DynamicInvocationExpressionsRewriter()
        {
        }

        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            var expression = node.Expression.ToString();
            switch (expression)
            {
                case "Enumerable.Range":
                    return HandleEnumerableRange(node);
                case "Enumerable.ToDictionary":
                    return HandleEnumerableToDictionary(node);
                case "Enumerable.Distinct":
                    return HandleEnumerableDistinct(node);

            }

            return base.VisitInvocationExpression(node);
        }

        private SyntaxNode HandleEnumerableDistinct(InvocationExpressionSyntax node)
        {
            return SyntaxFactory.ParseExpression($"((IEnumerable<dynamic>){node})");
        }

        private SyntaxNode HandleEnumerableRange(InvocationExpressionSyntax node)
        {
            var parentMethod = GetParentMethod(node);
            switch (parentMethod)
            {
                case "Select":
                case "Enumerable.ToDictionary":
                    return SyntaxFactory.ParseExpression($"{node}.Cast<dynamic>()");
            }

            return base.VisitInvocationExpression(node);
        }

        private SyntaxNode HandleEnumerableToDictionary(InvocationExpressionSyntax node)
        {
            return SyntaxFactory.ParseExpression($"((Dictionary<dynamic, dynamic>){node})");
        }

        private static string GetParentMethod(InvocationExpressionSyntax currentInvocation)
        {
            var member = currentInvocation.Parent as MemberAccessExpressionSyntax;
            if (member != null)
                return member.Name.Identifier.Text;

            var argument = currentInvocation.Parent as ArgumentSyntax;
            if (argument == null)
                return null;

            var argumentList = argument.Parent as ArgumentListSyntax;
            if (argumentList == null)
                return null;

            var invocation = argumentList.Parent as InvocationExpressionSyntax;
            if (invocation == null)
                return null;

            member = invocation.Expression as MemberAccessExpressionSyntax;
            return member?.ToString();
        }

        private void HandleMethod(string method)
        {
            switch (method)
            {
                case "Select":
                case "ToDictionary":
                case "ToLookup":
                case "GroupBy":
                case "OrderBy":
                case "OrderByDescending":
                case "ThenBy":
                case "ThenByDescending":
                case "Recurse":
                case "SelectMany":
                case "Sum":
                case "Average":
                case "Max":
                case "Min":
                case "Any":
                case "All":
                case "First":
                case "FirstOrDefault":
                case "Last":
                case "LastOfDefault":
                case "Single":
                case "Where":
                case "Count":
                case "LongCount":
                case "SingleOrDefault":
                case "Zip":
                case "Aggregate":
                case "Join":
                case "GroupJoin":
                case "TakeWhile":
                case "SkipWhile":
                    return;
            }

            return;
        }
    }
}
