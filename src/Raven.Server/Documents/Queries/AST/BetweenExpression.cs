namespace Raven.Server.Documents.Queries.AST
{
    public class BetweenExpression : QueryExpression
    {
        public QueryExpression Source;
        public ValueExpression Max;
        public ValueExpression Min;

        public BetweenExpression(QueryExpression source, ValueExpression min, ValueExpression max)
        {
            Source = source;
            Min = min;
            Max = max;
            Type = ExpressionType.Between;
        }

        public override string ToString()
        {
            return Source + " BETWEEN " + Min + " AND " + Max;
        }

        public override string GetText()
        {
            return ToString();
        }
    }
}
