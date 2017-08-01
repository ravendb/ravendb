
namespace Raven.Server.Documents.Queries.Parser
{
    public class ValueToken
    {
        public int TokenLength;
        public int TokenStart;
        public int EscapeChars;
        public ValueTokenType Type;
    }
}