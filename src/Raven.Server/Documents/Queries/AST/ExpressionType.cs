namespace Raven.Server.Documents.Queries.AST
{
    public enum ExpressionType
    {
        None,
        Field,
        Between,
        Binary,
        In,
        Value,
        Method,
        True,
        Negated,
        Pattern       
    }
}
