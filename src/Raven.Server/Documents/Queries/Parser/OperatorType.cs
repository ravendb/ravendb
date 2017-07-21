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
        Method,
        And,
        AndNot,
        Or,
        OrNot,
        Field
    }
}