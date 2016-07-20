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

                var resultsGroupByAndSelect = node.Expression as MemberAccessExpressionSyntax; // results.GroupBy(result => result.Type).Select

                if (resultsGroupByAndSelect == null)
                    return base.VisitInvocationExpression(node);

                var resultsGroupBy = resultsGroupByAndSelect.Expression as InvocationExpressionSyntax; // results.GroupBy(result => result.Type)

                if (resultsGroupBy == null)
                    return base.VisitInvocationExpression(node); 
                
                var arguments = resultsGroupBy.ArgumentList.Arguments; // result => result.Type

                if (arguments.Count != 1)
                    throw new InvalidOperationException("Incorrect number of arguments in group by expression");
                
                var groupByLambda = (SimpleLambdaExpressionSyntax)arguments[0].Expression;

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

                return base.VisitInvocationExpression(node);
            }
        }
    }
}