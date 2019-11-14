namespace Raven.Server.Documents.Queries.AST
{
    public class TimeSeriesBetweenExpression : BetweenExpression
    {
        public QueryExpression MinExpression;
        public QueryExpression MaxExpression;

        public TimeSeriesBetweenExpression(QueryExpression source, QueryExpression min, QueryExpression max) : base(source, null, null)
        {
            MinExpression = min;
            MaxExpression = max;
        }

        public override string ToString()
        {
            return Source + " between " + MinExpression + " and " + MaxExpression;
        }

        public override string GetText(IndexQueryServerSide parent)
        {
            return Source.GetText(parent) + " between " + MinExpression.GetText(parent) + " and " + MaxExpression.GetText(parent);
        }

        public override bool Equals(QueryExpression other)
        {
            if (!(other is TimeSeriesBetweenExpression be))
                return false;

            return Source.Equals(be.Source) && MaxExpression.Equals(be.MaxExpression) && MinExpression.Equals(be.MinExpression);
        }
    }
}
