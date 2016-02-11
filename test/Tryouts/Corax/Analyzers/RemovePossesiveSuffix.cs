using Raven.Server.Json;

namespace Tryouts.Corax.Analyzers
{
    public class RemovePossesiveSuffix : IFilter
    {
        public unsafe bool ProcessTerm(LazyStringValue source)
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