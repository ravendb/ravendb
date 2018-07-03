using System.Collections.Generic;
using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class CounterIncludesToken : QueryToken
    {
        private readonly string _parameterName;
        private readonly bool _all;

        private CounterIncludesToken(string parameterName, bool all = false)
        {
            _parameterName = parameterName;
            _all = all;
        }

        public static CounterIncludesToken Create(string parameterName)
        {
            return new CounterIncludesToken(parameterName);
        }

        public static CounterIncludesToken All()
        {
            return new CounterIncludesToken(null, true);
        }

        public override void WriteTo(StringBuilder writer)
        {
            writer
                .Append("counters(");

            if (_all == false)
            {
                writer
                    .Append("$")
                    .Append(_parameterName);
            }

            writer
                .Append(")");
        }
    }
}
