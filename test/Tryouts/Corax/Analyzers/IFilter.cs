using Raven.Server.Json;

namespace Tryouts.Corax.Analyzers
{
    public interface IFilter
    {
        bool ProcessTerm(LazyStringValue source);
    }
}