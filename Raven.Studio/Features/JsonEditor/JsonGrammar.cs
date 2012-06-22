using System;
using System.Net;
using ActiproSoftware.Text.Parsing.LLParser;
using ActiproSoftware.Text.Parsing.LLParser.Implementation;

namespace Raven.Studio.Features.JsonEditor
{
    public class JsonGrammar : Grammar
    {
        public JsonGrammar() : base("Json")
        {
            // see http://www.json.org/ for an EBNF definition of the Json language

            // The Json Tokens are defined in json.langproj at the root of the Studio project, and can be edited
            // using Actipro's language designer

            var @openCurly = new Terminal(JsonTokenId.OpenCurlyBrace, "OpenCurly") {ErrorAlias = "'{'"};
            var @closeCurly = new Terminal(JsonTokenId.CloseCurlyBrace, "CloseCurly") {ErrorAlias = "'}'"};

            var @openSquare = new Terminal(JsonTokenId.OpenSquareBrace, "OpenSquare") {ErrorAlias = "'['"};
            var @closeSquare = new Terminal(JsonTokenId.CloseSquareBrace, "CloseSquare") {ErrorAlias = "']'"};

            var @comma = new Terminal(JsonTokenId.Comma, "Comma") {ErrorAlias = "','"};
            var @colon = new Terminal(JsonTokenId.Colon, "Colon") {ErrorAlias = "':'"};

            var @number = new Terminal(JsonTokenId.Number, "Number");

            var @true = new Terminal(JsonTokenId.True, "True") {ErrorAlias = "true"};
            var @false = new Terminal(JsonTokenId.False, "False") {ErrorAlias = "false"};
            var @null = new Terminal(JsonTokenId.Null, "Null") {ErrorAlias = "null"};

            var @startString = new Terminal(JsonTokenId.StringStartDelimiter, "StartQuote") {ErrorAlias = "'\"'"};
            var @endString = new Terminal(JsonTokenId.StringEndDelimiter, "EndQuote") {ErrorAlias = "'\"'"};
            var @stringCharacters = new Terminal(JsonTokenId.StringText, "Characters");
            var @escapedCharacter = new Terminal(JsonTokenId.EscapedCharacter,"EscapeSequence"){ErrorAlias = "Escape sequence ('\\\"', '\\\\', '\\b', '\\f', '\\n', '\\r', '\\t'"};
            var @escapedUnicode = new Terminal(JsonTokenId.EscapedUnicode, "EscapedUnicodeCharacter") {ErrorAlias = "Unicode character (e.g. \\u12ab)"};
            
            var jsonObject = new NonTerminal("Object");
            var propertyValue = new NonTerminal("PropertyValue");
            var propertyValueList = new NonTerminal("PropertyValueList");
            var value = new NonTerminal("Value") { ErrorAlias = "Value"};
            var array = new NonTerminal("Array");
            var stringValue = new NonTerminal("String");

            jsonObject.Production = @openCurly + propertyValueList.OnError(AdvanceToStringOrClosingBrace).ZeroOrMore() + @closeCurly.OnErrorContinue();
            propertyValueList.Production = propertyValue + @comma.OnError(DontReportBeforeClosingBrace);
            propertyValue.Production = stringValue + @colon + value;

            value.Production = stringValue | @number | jsonObject | array | @true | @false | @null;
            array.Production = @openSquare + (value + (@comma + value).ZeroOrMore()).Optional() + @closeSquare.OnErrorContinue();

            stringValue.Production = @startString + (@stringCharacters | @escapedCharacter | @escapedUnicode).ZeroOrMore() + @endString.OnErrorContinue();

            this.Root = jsonObject;
        }

        private IParserErrorResult DontReportBeforeClosingBrace(IParserState state)
        {
            if (state.TokenReader.LookAheadToken.Id == JsonTokenId.CloseCurlyBrace)
            {
                return ParserErrorResults.Ignore;
            }
            else
            {
                return ParserErrorResults.Continue;
            }
        }

        private IParserErrorResult AdvanceToStringOrClosingBrace(IParserState state)
        {
            state.TokenReader.AdvanceTo(JsonTokenId.StringStartDelimiter, JsonTokenId.CloseCurlyBrace);
            return ParserErrorResults.Continue;
        }
    }
}
