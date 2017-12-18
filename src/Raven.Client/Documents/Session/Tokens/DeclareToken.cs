using System;
using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class DeclareToken : QueryToken
    {
        public string Name { get; }

        public string Parameters { get; }

        public string Body { get; }

        private DeclareToken(string name, string body, string parameters = null)
        {
            Name = name;
            Body = body;
            Parameters = parameters;
        }

        public static DeclareToken Create(string name, string body, string parameters = null)
        {
            return new DeclareToken(name, body, parameters);
        }

        public override void WriteTo(StringBuilder writer)
        {
            writer
                .Append("declare ")
                .Append("function ")
                .Append(Name)
                .Append("(")
                .Append(Parameters)
                .Append(") ")
                .Append("{")
                .Append(Environment.NewLine)
                .Append(Body)
                .Append(Environment.NewLine)
                .Append("}")
                .Append(Environment.NewLine);
        }
    }
}
