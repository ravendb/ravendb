using Sparrow;

namespace Raven.Server.Documents.Queries.Parser
{
    public class FieldToken
    {
        public int EscapeChars;
        public StringSegment Token;
        public bool IsQuoted;
    }
}
