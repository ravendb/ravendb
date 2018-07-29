using System;
using System.Collections.Generic;
using System.Text;

namespace Raven.Server.Documents.Queries.AST
{
    public class WithEdgesExpression : QueryExpression
    {
        public QueryExpression Where;

        public List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> OrderBy;

        public WithEdgesExpression(QueryExpression @where, List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> orderBy)
        {
            if(@where == null && orderBy == null)
                throw new ArgumentNullException($"{nameof(WithEdgesExpression)} should have either Where or OrderBy clauses.");

            Where = @where;
            OrderBy = orderBy;
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
