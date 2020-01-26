using System;
using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class DeclareToken : QueryToken
    {
        public string Name { get; }

        public string Parameters { get; }

        public string Body { get; }

        private readonly bool _timeSeries;

        private DeclareToken(string name, string body, string parameters, bool timeSeries)
        {
            Name = name;
            Body = body;
            Parameters = parameters;
            _timeSeries = timeSeries;
        }

        public static DeclareToken CreateFunction(string name, string body, string parameters = null)
        {
            return new DeclareToken(name, body, parameters, timeSeries: false);
        }

        public static DeclareToken CreateTimeSeries(string name, string body, string parameters = null)
        {
            return new DeclareToken(name, body, parameters, timeSeries: true);
        }

        public override void WriteTo(StringBuilder writer)
        {
            writer
                .Append("declare ")
                .Append(_timeSeries ? "timeseries " : "function ")
                .Append(Name)
                .Append('(')
                .Append(Parameters)
                .Append(") ")
                .Append('{')
                .Append(Environment.NewLine)
                .Append(Body)
                .Append(Environment.NewLine)
                .Append('}')
                .Append(Environment.NewLine);
        }
    }


}
