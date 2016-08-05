using System;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters.ReduceIndex
{
    public abstract class GroupByFieldsRetriever : CSharpSyntaxRewriter
    {
        public string[] GroupByFields { get; protected set; }

        public static GroupByFieldsRetriever QuerySyntax => new QuerySyntaxRetriever();

        public static GroupByFieldsRetriever MethodSyntax => new MethodSyntaxRetriever();

        public class QuerySyntaxRetriever : GroupByFieldsRetriever
        {
            public override SyntaxNode VisitGroupClause(GroupClauseSyntax node)
            {
                var result = node.GroupExpression.ToFullString().Trim();
                var by = node.ByExpression.ToFullString();

                if (by.StartsWith("new"))
                {
                    by = by.Substring(3);
                    by = by.Trim('{', ' ', '}');
                }

                by = by.Replace($"{result}.", string.Empty);

                GroupByFields = by.Split(',');

                for (int i = 0; i < GroupByFields.Length; i++)
                {
                    GroupByFields[i] = GroupByFields[i].Trim();
                }

                return base.VisitGroupClause(node);
            }
        }

        private class MethodSyntaxRetriever : GroupByFieldsRetriever
        {
            public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
            {
                var expression = node.Expression.ToString();
                if (expression.StartsWith("results.GroupBy") == false)
                    return base.VisitInvocationExpression(node);

                var groupByLambda = node.Expression.DescendantNodes(x => true)
                    .FirstOrDefault(x => x.IsKind(SyntaxKind.SimpleLambdaExpression)) as SimpleLambdaExpressionSyntax;

                if (groupByLambda == null)
                    throw new InvalidOperationException("Could not extract arguments from group by expression");

                var argument = groupByLambda.Parent as ArgumentSyntax;
                if (argument == null)
                    throw new InvalidOperationException("Could not extract arguments from group by expression");

                var arguments = argument.Parent as ArgumentListSyntax;

                if (arguments == null)
                    throw new InvalidOperationException("Could not extract arguments from group by expression");

                if (arguments.Arguments.Count != 1)
                    throw new InvalidOperationException("Incorrect number of arguments in group by expression");

                var singleGroupByField = groupByLambda.Body as MemberAccessExpressionSyntax;
                var multipleGroupByFields = groupByLambda.Body as AnonymousObjectCreationExpressionSyntax;

                if (singleGroupByField != null)
                {
                    GroupByFields = new[] { singleGroupByField.Name.Identifier.ValueText };
                }
                else if (multipleGroupByFields != null)
                {
                    GroupByFields = RewritersHelper.ExtractFields(multipleGroupByFields).ToArray();
                }
                else
                {
                    throw new InvalidOperationException("Could not extract group by fields");
                }

                return node;
            }
        }
    }
}