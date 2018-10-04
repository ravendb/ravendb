using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Client.Exceptions.Documents.Compilation;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters.ReduceIndex
{
    public abstract class MethodsInGroupByValidator : CSharpSyntaxWalker
    {
        protected static string[] ForbiddenMethods = { "Count", "Average" };

        protected Dictionary<string, string> SearchPatterns = new Dictionary<string, string>();

        public static MethodsInGroupByValidator MethodSyntaxValidator => new MethodsInGroupByValidatorMethodSyntax();
        public static MethodsInGroupByValidator QuerySyntaxValidator => new MethodsInGroupByValidatorQuerySyntax();

        public void Start(ExpressionSyntax node)
        {
            Visit(node.SyntaxTree.GetRoot());
        }
    }

    public class MethodsInGroupByValidatorMethodSyntax : MethodsInGroupByValidator
    {
        private ParameterSyntax _root;

        public void SetSearchPatterns()
        {
            SearchPatterns.Clear();
            foreach (var searchTerm in ForbiddenMethods)
            {
                SearchPatterns.Add(searchTerm, $"Enumerable.{searchTerm}({_root.Identifier})");
            }
        }

        public override void VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            if (node.Expression is MemberAccessExpressionSyntax exp && exp.ToString().EndsWith("Select"))
            {
                // find GroupBy(...).Select(root => .. )
                if (exp.Expression is InvocationExpressionSyntax group && group.Expression.ToString().EndsWith("GroupBy"))
                {
                    // we should be in the right place
                    var myLambda = node.ArgumentList.Arguments.First().Expression as SimpleLambdaExpressionSyntax;
                    if (myLambda == null)
                    {
                        throw new IndexCompilationException("Select expression must contain parameter(s)");
                    }

                    _root = myLambda.Parameter;
                    SetSearchPatterns();

                    var candidates = myLambda.DescendantNodes()
                        .Where(n => n.IsKind(SyntaxKind.InvocationExpression)).ToList();

                    foreach (var syntaxNode in candidates)
                    {
                        var str = syntaxNode.ToFullString();
                        foreach (var searchPattern in SearchPatterns)
                        {
                            if (str.Contains(searchPattern.Value))
                            {
                                throw new IndexCompilationException($"Reduce cannot contain {searchPattern.Key}() methods in grouping.");
                            }
                        }
                    }

                    return;
                }
            }

            base.VisitInvocationExpression(node);
        }
    }

    public class MethodsInGroupByValidatorQuerySyntax : MethodsInGroupByValidator
    {
        private SyntaxToken _root;

        public override void VisitQueryContinuation(QueryContinuationSyntax node)
        {
            if (_root == default && node.IntoKeyword.ToString().Contains("into"))
            {
                _root = node.Identifier; // get the into object
                SetSearchPatterns();
            }

            base.VisitQueryContinuation(node);
        }

        public override void VisitFromClause(FromClauseSyntax node)
        {
            if (_root != default)
            {
                base.VisitFromClause(node);
            }

            // else skip the from clause
        }

        public void SetSearchPatterns()
        {
            SearchPatterns.Clear();
            foreach (var searchTerm in ForbiddenMethods)
            {
                SearchPatterns.Add(searchTerm, $"{_root}.{searchTerm}()");
            }
        }

        public override void VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            if (_root != default)
            {
                foreach (var searchTerm in SearchPatterns)
                {
                    if (node.ToString().Contains(searchTerm.Value))
                    {
                        throw new IndexCompilationException($"Reduce cannot contain {searchTerm.Key}() methods in grouping.");
                    }
                }
            }

            base.VisitInvocationExpression(node);
        }
    }

    public abstract class GroupByFieldsRetriever : CSharpSyntaxRewriter
    {
        public CompiledIndexField[] GroupByFields { get; protected set; }

        public static GroupByFieldsRetriever QuerySyntax => new QuerySyntaxRetriever();

        public static GroupByFieldsRetriever MethodSyntax => new MethodSyntaxRetriever();

        public class QuerySyntaxRetriever : GroupByFieldsRetriever
        {
            public override SyntaxNode VisitGroupClause(GroupClauseSyntax node)
            {
                var groupByFields = new List<CompiledIndexField>();

                // by new { ... }
                FindGroupByFields(node.ByExpression, groupByFields);

                GroupByFields = groupByFields.ToArray();

                return base.VisitGroupClause(node);
            }

            private void FindGroupByFields(ExpressionSyntax expr, List<CompiledIndexField> groupByFields)
            {
                switch (expr)
                {
                    case AnonymousObjectCreationExpressionSyntax aoc:
                        foreach (var initializer in aoc.Initializers)
                        {
                            if (initializer.Expression is MemberAccessExpressionSyntax mae)
                            {
                                groupByFields.Add(new SimpleField(mae.Name.Identifier.Text));
                            }
                            else
                            {
                                throw new InvalidOperationException("Unable to understand expression " + initializer);
                            }
                        }

                        break;
                    case MemberAccessExpressionSyntax mae:
                        groupByFields.Add(new SimpleField(mae.Name.Identifier.Text));
                        break;
                    case ParenthesizedExpressionSyntax pes:
                        FindGroupByFields(pes.Expression, groupByFields);
                        break;
                    case CastExpressionSyntax ces:
                        FindGroupByFields(ces.Expression, groupByFields);
                        break;
                    case LiteralExpressionSyntax _:
                        // explicitly ignore, we don't need to do anything here
                        break;
                    default:
                        throw new InvalidOperationException("Unable to understand expression " + expr);
                }
            }
        }

        private class MethodSyntaxRetriever : GroupByFieldsRetriever
        {
            public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
            {
                var expression = node.Expression.ToString();
                if (expression.StartsWith("results.") == false || expression.EndsWith(".GroupBy") == false)
                    return base.VisitInvocationExpression(node);

                var groupByLambda = node.ArgumentList.DescendantNodes(x => true)
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
                    GroupByFields = new[] { RewritersHelper.ExtractField(singleGroupByField) };
                }
                else if (multipleGroupByFields != null)
                {
                    GroupByFields = RewritersHelper.ExtractFields(multipleGroupByFields, retrieveOriginal: true, nestFields: true).ToArray();
                }
                else if (literalGroupByField != null)
                {
                    GroupByFields = new CompiledIndexField[0];
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
