using System;
using System.Collections.Generic;
using System.Text;
using JetBrains.Annotations;
using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    public class WithEdgesExpression : QueryExpression
    {
        public QueryExpression Where;

        public StringSegment EdgeType;

        public List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> OrderBy;

        public WithEdgesExpression(QueryExpression @where, [NotNull] string edgeType, List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> orderBy)
        {
            if(@where == null && orderBy == null)
                throw new ArgumentNullException($"{nameof(WithEdgesExpression)} should have either Where or OrderBy clauses.");

            Where = @where;
            OrderBy = orderBy;
            EdgeType = edgeType ?? throw new ArgumentNullException(nameof(edgeType));
            Type = ExpressionType.WithEdge;
        }

        public override string ToString() => GetText();
        public override string GetText(IndexQueryServerSide parent) => GetText();

        private string GetText()
        {
            var sb = new StringBuilder();

            var visitor = new StringQueryVisitor(sb);

            if (Where != null)
            {
                visitor.VisitWhereClause(Where);
            }

            if (OrderBy != null)
            {
                visitor.VisitOrderBy(OrderBy);
            }

            return sb.ToString();
        }
    }
}
