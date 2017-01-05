using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Abstractions.Extensions;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters.ReduceIndex
{
    public class MethodsInGroupByValidator : CSharpSyntaxWalker
    {
        private readonly string[] _searchTerms;

        public MethodsInGroupByValidator(string[] searchTerms)
        {
            _searchTerms = searchTerms.Where(s => !String.IsNullOrEmpty(s)).Distinct().ToArray();

            if (_searchTerms.Length == 0)
            {
                throw new ArgumentNullException(nameof(_searchTerms),"Cannot be empty");
            }      
        }
       
        public void Start(ExpressionSyntax node)
        {
            Visit(node.SyntaxTree.GetRoot());
        }

        private SyntaxToken _root;
        public override void VisitQueryContinuation(QueryContinuationSyntax node)
        {
            if (_root == default(SyntaxToken) && node.IntoKeyword.ToString().Contains("into"))
            {
                _root = node.Identifier; // get the into object
            }
            base.VisitQueryContinuation(node);
        }

        public override void VisitFromClause(FromClauseSyntax node)
        {
            
        }

        public override void VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            if (_root != default(SyntaxToken))
            {
                foreach (var searchTerm in _searchTerms)
                {
                    var search = $"{_root}.{searchTerm}()";
                    if (node.ToString().Contains(search))
                    {
                        throw new Exception($"Expression cannot contain {searchTerm}() methods in grouping.");
                    }
                }
            }
            base.VisitInvocationExpression(node);
        }
    }




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
                    by = by.Trim('{', ' ', '}', '\r', '\n');
                }

                by = by.Replace($"{result}.", string.Empty);

                GroupByFields = by.Split(',');

                for (int i = 0; i < GroupByFields.Length; i++)
                {
                    var field = GroupByFields[i];

                    var parts = field.Split('=');

                    GroupByFields[i] = parts[0].Trim();
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
                var literalGroupByField = groupByLambda.Body as LiteralExpressionSyntax;

                if (singleGroupByField != null)
                {
                    GroupByFields = new[] { singleGroupByField.Name.Identifier.ValueText };
                }
                else if (multipleGroupByFields != null)
                {
                    GroupByFields = RewritersHelper.ExtractFields(multipleGroupByFields).ToArray();
                }
                else if (literalGroupByField != null)
                {
                    GroupByFields = new string[0];
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