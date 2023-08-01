using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public sealed class HighlightingToken : QueryToken
    {
        private readonly string _fieldName;
        private readonly int _fragmentLength;
        private readonly int _fragmentCount;
        private readonly string _optionsParameterName;

        private HighlightingToken(string fieldName, int fragmentLength, int fragmentCount, string optionsParameterName)
        {
            _fieldName = fieldName;
            _fragmentLength = fragmentLength;
            _fragmentCount = fragmentCount;
            _optionsParameterName = optionsParameterName;
        }

        public static HighlightingToken Create(string fieldName, int fragmentLength, int fragmentCount, string optionsParameterName)
        {
            return new HighlightingToken(fieldName, fragmentLength, fragmentCount, optionsParameterName);
        }

        public override void WriteTo(StringBuilder writer)
        {
            writer
                .Append("highlight(");

            WriteField(writer, _fieldName);

            writer
                .Append(",")
                .Append(_fragmentLength)
                .Append(",")
                .Append(_fragmentCount);

            if (_optionsParameterName != null)
            {
                writer
                    .Append(",$")
                    .Append(_optionsParameterName);
            }

            writer
                .Append(")");
        }
    }
}
