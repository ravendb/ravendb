using System;
using System.Collections.Generic;
using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class GraphQueryToken : QueryToken
    {
        private string _query;

        public GraphQueryToken(string query)
        {
            _query = query;
        }
        public override void WriteTo(StringBuilder writer)
        {
            writer.Append(_query);
        }
    }
}
