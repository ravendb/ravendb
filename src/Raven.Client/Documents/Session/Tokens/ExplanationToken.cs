using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public sealed class ExplanationToken : QueryToken
    {
        private readonly string _optionsParameterName;

        private ExplanationToken(string optionsParameterName)
        {
            _optionsParameterName = optionsParameterName;
        }

        public static ExplanationToken Create(string optionsParameterName)
        {
            return new ExplanationToken(optionsParameterName);
        }

        public override void WriteTo(StringBuilder writer)
        {
            writer
                .Append("explanations(");

            if (_optionsParameterName != null)
            {
                writer
                    .Append("$")
                    .Append(_optionsParameterName);
            }

            writer
                .Append(")");
        }
    }
}
