using Raven.Abstractions.Data;

namespace Raven.Database.Queries
{
    public static class SuggestionQueryExtensions
    {
        public static SuggestionQueryResult ExecuteSuggestionQuery(this DocumentDatabase self, string index, SuggestionQuery suggestionQuery)
        {
            return new SuggestionQueryRunner(self).ExecuteSuggestionQuery(index, suggestionQuery);
        }
    }
}