using System;
using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class DeclareToken : QueryToken
    {
        public enum DeclarationType
        {
            Function,
            TimeSeries
        }

        public string Name { get; }

        public string Parameters { get; }

        public string Body { get; }

        public DeclarationType Type { get; }

        private DeclareToken(string name, string body, string parameters, DeclarationType type)
        {
            Name = name;
            Body = body;
            Parameters = parameters;
            Type = type;
        }

        public static DeclareToken Create(string name, string body, string parameters = null, DeclarationType type = DeclarationType.Function)
        {
            return new DeclareToken(name, body, parameters, type);
        }

        public override void WriteTo(StringBuilder writer)
        {
            writer
                .Append("declare ")
                .Append(Type == DeclarationType.Function ? "function " : "timeseries ")
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
