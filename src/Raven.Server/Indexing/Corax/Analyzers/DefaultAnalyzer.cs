using Raven.Server.Indexing.Corax.Analyzers.Filters;

namespace Raven.Server.Indexing.Corax.Analyzers
{
    public class DefaultAnalyzer : IAnalyzer
    {
        readonly IFilter[] _filters =
        {
            new LowerCaseFilter(),
            new RemovePossesiveSuffix(),
            new StopWordsFilter(),
        };

        public ITokenSource CreateTokenSource(string field, ITokenSource existing)
        {
            var tokenSource = existing ?? new StringTokenizer();
            tokenSource.Position = 0;
            return tokenSource;
        }

        public bool Process(string field, ITokenSource source)
        {
            for (int i = 0; i < _filters.Length; i++)
            {
                if (_filters[i].ProcessTerm(source) == false)
                    return false;
            }
            return true;
        }
    }
}