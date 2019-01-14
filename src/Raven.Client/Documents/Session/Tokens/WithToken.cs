using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents.Linq;

namespace Raven.Client.Documents.Session.Tokens
{
    public class WithToken<T>: QueryToken
    {
        private string alias;
        private readonly IRavenQueryable<T> _query;

        public WithToken(string alias, IRavenQueryable<T> query)
        {
            this.alias = alias;
            this._query = query;
        }

        public override void WriteTo(StringBuilder writer)
        {
            //TODO: Validate _query has no invalid clauses like 'select' and others
            writer.Append($"with {{{_query}}} as {alias}");
        }
    }
}
