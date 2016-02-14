namespace Raven.Server.Indexing.Corax.Analyzers.Filters
{
    public class LowerCaseFilter : IFilter
    {
        public bool ProcessTerm(ITokenSource source)
        {
            for (int i = 0; i < source.Size; i++)
            {
                source.Buffer[i] = char.ToLowerInvariant(source.Buffer[i]);
            }
            return true;
        }
    }
}