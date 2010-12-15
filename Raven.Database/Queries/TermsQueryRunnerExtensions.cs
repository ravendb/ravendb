using System.Collections.Generic;

namespace Raven.Database.Queries
{
    public static class TermsQueryRunnerExtensions
    {
        public static IDictionary<string, HashSet<string>> GetTerms(this DocumentDatabase self, string index, string field)
        {
            return new TermsQueryRunner(self).GetTerms(index, field);
        }
    }
}