using System;
using System.Collections.Generic;

namespace Raven.Database.Queries
{
    public static class TermsQueryRunnerExtensions
    {
        public static ISet<string> GetTerms(this DocumentDatabase self, string index, string field, string fromValue, int pageSize)
        {
            return new TermsQueryRunner(self).GetTerms(index, field, fromValue, Math.Min(pageSize, self.Configuration.MaxPageSize));
        }
    }
}