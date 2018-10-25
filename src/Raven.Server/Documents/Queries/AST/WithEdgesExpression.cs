using System.Collections.Generic;
using System.Text;
using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    public class WithEdgesExpression : QueryExpression
    {
        public QueryExpression Where;

        public StringSegment? FromAlias;

        public FieldExpression Path;

        public List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> OrderBy;

        public StringSegment EdgeAlias;

        public WithEdgesExpression(QueryExpression @where, FieldExpression path, List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> orderBy)
        {
            Where = @where;
            OrderBy = orderBy;
            Type = ExpressionType.WithEdge;
            Path = path;
        }

        public override string ToString() => GetText();
        public override string GetText(IndexQueryServerSide parent) => GetText();

        private string GetText()
        {
            var sb = new StringBuilder();

            var visitor = new StringQueryVisitor(sb);

            visitor.VisitWithEdgesExpression(FromAlias?.ToString(),this);

            return sb.ToString();
        }

       
        public override bool Equals(QueryExpression other)
        {
            if (!(other is WithEdgesExpression ie))
                return false;

            if (Path != ie.Path)
                return false;

            if ((Where != null) != (ie.Where != null) || 
                (OrderBy != null) != (ie.OrderBy != null))
                return false;

            if (Where != null && Where.Equals(ie.Where) == false)
                return false;

            if(OrderBy != null)
            {
                if (OrderBy.Count != ie.OrderBy.Count)
                    return false;

                for (int i = 0; i < OrderBy.Count; i++)
                {
                    if (OrderBy[i].Equals(ie.OrderBy.Count) == false)
                        return false;
                }
            }

            return true;
        }
    }
}
