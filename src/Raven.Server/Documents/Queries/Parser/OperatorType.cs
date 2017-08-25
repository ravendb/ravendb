namespace Raven.Server.Documents.Queries.Parser
{
    public enum OperatorType
    {
        Equal,
        NotEqual,
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
