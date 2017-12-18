using System;
using System.Collections.Generic;
using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public abstract class QueryToken
    {
        public abstract void WriteTo(StringBuilder writer);

        protected  void WriteField(StringBuilder writer, string field)
        {
            var keyWord = RqlKeywords.Contains(field);

            if (keyWord)
                writer.Append("'");

            writer.Append(field);

            if (keyWord)
                writer.Append("'");
        }

        private static readonly HashSet<string> RqlKeywords = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "as",
            "select",
            "where",
            "load",
            "group",
            "order",
            "include"
        };
    }
}
