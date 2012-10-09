using ActiproSoftware.Text.Parsing.LLParser.Implementation;

namespace Raven.Studio.Features.JsonEditor
{
    public class JsonParser : LLParserBase
    {
        public JsonParser() : base(new JsonGrammar())
        {
        }

        public override ActiproSoftware.Text.Parsing.LLParser.ITokenReader CreateTokenReader(ActiproSoftware.Text.ITextBufferReader reader)
        {
            return new JsonTokenReader(reader, new JsonLexer(new JsonClassificationTypeProvider()));
        }
    }
}