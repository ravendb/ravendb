namespace Raven.Server.Documents.Queries.AST
{
    public class BetweenExpression : QueryExpression
    {
        public QueryExpression Source;
        public ValueExpression Max;
        public ValueExpression Min;
        public bool MinInclusive = true;
        public bool MaxInclusive = true;

        public BetweenExpression(QueryExpression source, ValueExpression min, ValueExpression max)
        {
            Source = source;
            Min = min;
            Max = max;
            Type = ExpressionType.Between;
        }

        public override string ToString()
        {
            return Source + " between " + Min + " and " + Max;
        }

        public override string GetText(IndexQueryServerSide parent)
        {
            return Source.GetText(parent) + " between " + Min.GetText(parent) + " and " + Max.GetText(parent);
        }

        public override bool Equals(QueryExpression other)
        {
            if (!(other is BetweenExpression be))
                return false;

            return Source.Equals(be.Source) && Max.Equals(be.Max) && Min.Equals(be.Min);
        }
    }
}
