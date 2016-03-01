namespace Raven.Server.Indexing.Corax.Analyzers.Filters
{
    public class RemovePossesiveSuffix : IFilter
    {
        public bool ProcessTerm(ITokenSource source)
        {
            if (source.Size <= 2)
                return true;
            if (source.Buffer[source.Size - 1] == '\'') // remove "boys' ball" suffix '
            {
                source.Size--;
            }
            // remove "boy's ball" suffix 's
            else if ((source.Buffer[source.Size - 1] == 's') && source.Buffer[source.Size - 2] == '\'')
            {
                source.Size -= 2;
            }
            return true;
        }
    }
}