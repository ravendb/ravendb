namespace Raven.Server.Documents.Queries.Parser
{
    public enum OperatorType
    {
        Equal,
        LessThan,
        GreaterThan,
        LessThanEqual,
        GreaterThanEqual,
        Between,
        In,
        AllIn,
        Method,
        And,
        AndNot,
        Or,
        OrNot,
        Field,
        True,
        Value
    }
}
