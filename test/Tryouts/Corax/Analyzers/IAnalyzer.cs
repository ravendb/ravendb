using Raven.Server.Json;

namespace Tryouts.Corax.Analyzers
{
    public interface IAnalyzer
    {
        ITokenSource CreateTokenSource(LazyStringValue field, ITokenSource existing);
        bool Process(LazyStringValue field, LazyStringValue source);
    }
}