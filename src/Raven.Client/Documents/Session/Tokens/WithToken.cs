using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents.Linq;

namespace Raven.Client.Documents.Session.Tokens
{
    public class WithToken: QueryToken
    {
        private readonly string _alias;
        private readonly string _query;

        public WithToken(string alias, string query)
        {
            _alias = alias;
            _query = query;
        }

        public override void WriteTo(StringBuilder writer)
        {
            writer.Append("with {");
            writer.Append(_query);
            writer.Append("} as ");
            writer.Append(_alias);
        }
    }
}
