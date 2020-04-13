using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.Documents.Queries.AST
{
    public class TimeSeriesBetweenExpression : BetweenExpression
    {
        public QueryExpression MinExpression { get; }

        public QueryExpression MaxExpression { get; }

        public  List<FieldExpression> FromList { get; }

        public TimeSeriesBetweenExpression(List<FieldExpression> fromList, QueryExpression min, QueryExpression max) : base(null, null, null)
        {
            MinExpression = min;
            MaxExpression = max;
            FromList = fromList;
        }

        public override string ToString()
        {
            var fromListText = $"({string.Join(",", FromList)})";
            return fromListText + " between " + MinExpression + " and " + MaxExpression;
        }

        public override string GetText(IndexQueryServerSide parent)
        {
            return Source.GetText(parent) + " between " + MinExpression.GetText(parent) + " and " + MaxExpression.GetText(parent);
        }

        public override bool Equals(QueryExpression other)
        {
            if (!(other is TimeSeriesBetweenExpression be))
                return false;

            return FromList.SequenceEqual(be.FromList) && MaxExpression.Equals(be.MaxExpression) && MinExpression.Equals(be.MinExpression);
        }
    }
}
