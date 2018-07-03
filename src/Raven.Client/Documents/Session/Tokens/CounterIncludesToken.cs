using System.Collections.Generic;
using System.Text;
using Raven.Client.Extensions;

namespace Raven.Client.Documents.Session.Tokens
{
    public class CounterIncludesToken : QueryToken
    {
        private readonly Parameters _queryParameters;
        private readonly HashSet<string> _counters;

        private CounterIncludesToken(HashSet<string> countres, Parameters queryParameters)
        {
            _counters = countres;
            _queryParameters = queryParameters;
        }

        public static CounterIncludesToken Create(HashSet<string> countres, Parameters queryParameters)
        {
            return new CounterIncludesToken(countres, queryParameters);
        }

        public override void WriteTo(StringBuilder writer)
        {
            writer
                .Append("counters(");

            if (_queryParameters != null)
            {
                var parameterName = $"p{_queryParameters.Count.ToInvariantString()}";
                _queryParameters.Add(parameterName, _counters);

                writer
                    .Append("$")
                    .Append(parameterName);
            }


            writer
                .Append(")");
        }
    }
}
