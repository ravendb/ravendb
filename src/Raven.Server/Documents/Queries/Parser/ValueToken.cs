

using Sparrow;

namespace Raven.Server.Documents.Queries.Parser
{
    public class ValueToken
    {
        public int EscapeChars;
        public StringSegment Token;
        public ValueTokenType Type;
    }
}
