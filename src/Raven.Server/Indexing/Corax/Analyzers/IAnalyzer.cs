using Raven.Server.Json;

namespace Tryouts.Corax.Analyzers
{
    public interface IAnalyzer
    {
        ITokenSource CreateTokenSource(string field, ITokenSource existing);
        bool Process(string field, ITokenSource source);
    }
}