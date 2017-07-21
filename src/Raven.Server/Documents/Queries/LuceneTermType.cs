namespace Raven.Server.Documents.Queries.Parser.Lucene
{
    public enum LuceneTermType
    {
        Quoted,
        QuotedWildcard,
        UnQuoted,
        Float,
        Double,
        DateTime,
        Int,
        Long,
        UnAnalyzed,
        Null,
        WildCardTerm,
        PrefixTerm,
        Hex
    }
}