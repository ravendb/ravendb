namespace Raven.Server.Documents.Queries.AST
{
    public enum OrderByFieldType
    {
        Implicit,
        String,
        Long,
        Double,
        AlphaNumeric,
        Random,
        Score,
        Distance,
        Custom
    }
}
