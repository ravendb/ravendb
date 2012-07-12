using System;
using System.Net;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Lexing;
using ActiproSoftware.Text.Parsing.LLParser.Implementation;

namespace Raven.Studio.Features.JsonEditor
{
    public class JsonTokenReader : MergableTokenReader
    {
        public JsonTokenReader(ITextBufferReader reader, IMergableLexer rootLexer) : base(reader, rootLexer)
        {
        }

        protected override IToken GetNextToken()
        {
            IToken token = base.GetNextToken();

            // Loop to skip over tokens that are insignificant to the parser
            while (!this.IsAtEnd)
            {
                switch (token.Id)
                {
                    case JsonTokenId.Whitespace:
                        // Skip
                        token = base.GetNextToken();
                        break;
                    default:
                        return token;
                }
            }

            return token;

        }
    }
}
