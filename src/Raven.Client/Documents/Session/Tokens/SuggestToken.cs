using System;
using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class SuggestToken : QueryToken
    {
        public readonly string FieldName;
        private readonly string _termParameterName;
        private readonly string _optionsParameterName;

        private SuggestToken(string fieldName, string termParameterName, string optionsParameterName)
        {
            FieldName = fieldName ?? throw new ArgumentNullException(nameof(fieldName));
            _termParameterName = termParameterName ?? throw new ArgumentNullException(nameof(termParameterName));
            _optionsParameterName = optionsParameterName;
        }

        public static SuggestToken Create(string fieldName, string termParameterName, string optionsParameterName)
        {
            return new SuggestToken(fieldName, termParameterName, optionsParameterName);
        }

        public override void WriteTo(StringBuilder writer)
        {
            writer
                .Append("suggest(")
                .Append(FieldName)
                .Append(", $")
                .Append(_termParameterName);

            if (_optionsParameterName != null)
            {
                writer
                    .Append(", $")
                    .Append(_optionsParameterName);
            }

            writer.Append(")");
        }
    }
}
