using System;
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
            // TODO arek
            //public override SyntaxNode VisitGroupClause(GroupClauseSyntax node)
            //{
            //    return base.VisitGroupClause(node);
            //}

            public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
            {
                throw new NotImplementedException("TODO arek");
            }
        }
    }
}