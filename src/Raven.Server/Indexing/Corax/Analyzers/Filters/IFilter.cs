namespace Raven.Server.Indexing.Corax.Analyzers.Filters
{
    public interface IFilter
    {
        bool ProcessTerm(ITokenSource source);
    }
}