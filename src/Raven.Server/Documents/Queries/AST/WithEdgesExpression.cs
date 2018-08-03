using System;
using System.Collections.Generic;
using System.Text;
using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    public class WithEdgesExpression : QueryExpression
    {
        public QueryExpression Where;

        public StringSegment? EdgeType;

        public List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> OrderBy;

        public WithEdgesExpression(QueryExpression @where, string edgeType, List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> orderBy)
        {
            Where = @where;
            OrderBy = orderBy;
            //null edges means all edges 
            EdgeType = edgeType;
            Type = ExpressionType.WithEdge;
        }

        public override string ToString() => GetText();
        public override string GetText(IndexQueryServerSide parent) => GetText();

        private string GetText()
        {
            var sb = new StringBuilder("WITH EDGES");
            sb.Append("(").Append(EdgeType).Append(")");

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
