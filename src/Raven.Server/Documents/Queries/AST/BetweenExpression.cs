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

        // the two comparisons the between stands for, each with its own inclusivity; a between written in RQL is inclusive on
        // both ends, one folded from 'Foo > $a and Foo < $b' is not
        public BinaryExpression LowerBound() => new BinaryExpression(Source, Min, MinInclusive ? OperatorType.GreaterThanEqual : OperatorType.GreaterThan);

        public BinaryExpression UpperBound() => new BinaryExpression(Source, Max, MaxInclusive ? OperatorType.LessThanEqual : OperatorType.LessThan);

        public override string ToString()
        {
            return Source + " between " + Min + " and " + Max;
        }

        public override string GetText(IndexQueryServerSide parent)
        {
            return Source.GetText(parent) + " between " + Min.GetText(parent) + " and " + Max.GetText(parent);
        }

        public override string GetTextWithAlias(IndexQueryServerSide parent)
        {
            return Source.GetTextWithAlias(parent) + " between " + Min.GetTextWithAlias(parent) + " and " + Max.GetTextWithAlias(parent);
        }

        public override bool Equals(QueryExpression other)
        {
            if (!(other is BetweenExpression be))
                return false;

            return Source.Equals(be.Source) && Max.Equals(be.Max) && Min.Equals(be.Min);
        }
    }
}
