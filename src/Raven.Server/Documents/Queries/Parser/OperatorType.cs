namespace Raven.Server.Documents.Queries.Parser
{
    public enum OperatorType
    {
        Equal,
        LessThen,
        GreaterThen,
        LessThenEqual,
        GreaterThenEqual,
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