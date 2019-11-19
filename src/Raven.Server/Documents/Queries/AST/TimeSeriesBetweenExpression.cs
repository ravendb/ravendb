namespace Raven.Server.Documents.Queries.AST
{
    public class TimeSeriesBetweenExpression : BetweenExpression
    {
        public QueryExpression MinExpression => Min ?? _minExpression;
        public QueryExpression MaxExpression => Max ?? _maxExpression;

        private readonly QueryExpression _minExpression;
        private readonly QueryExpression _maxExpression;

        public TimeSeriesBetweenExpression(QueryExpression source, QueryExpression min, QueryExpression max) : base(source, null, null)
        {
            if (min is ValueExpression ve)
            {
                Min = ve;
            }
            else
            {
                _minExpression = min;
            }

            if (max is ValueExpression ve2)
            {
                Max = ve2;
            }
            else
            {
                _maxExpression = max;
            }
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
