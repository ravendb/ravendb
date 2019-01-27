using System;
using System.Collections.Generic;
using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class WithEdgesToken : QueryToken
    {
        private readonly string _alias;
        private readonly string _edgeSelector;
        private readonly string _query;

        public WithEdgesToken(string alias, string edgeSelector, string query)
        {
            _alias = alias;
            _query = query;
            _edgeSelector = edgeSelector;
        }

        public override void WriteTo(StringBuilder writer)
        {
            writer.Append("with edges(");
            writer.Append(_edgeSelector);
            writer.Append(")");
            if (string.IsNullOrWhiteSpace(_query) == false)
            {
                writer.Append(" {");
                writer.Append(_query);
                writer.Append("} ");
            }
            writer.Append(" as ");
            writer.Append(_alias);
        }
    }
}
