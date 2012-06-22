using System;
using System.Net;

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

            var @number = new Terminal(JsonTokenId.Number, "Integer");

            var @true = new Terminal(JsonTokenId.True, "True") {ErrorAlias = "true"};
            var @false = new Terminal(JsonTokenId.False, "False") {ErrorAlias = "false"};
            var @null = new Terminal(JsonTokenId.Null, "Null") {ErrorAlias = "null"};

            var @startString = new Terminal(JsonTokenId.StringStartDelimiter, "StartQuote") {ErrorAlias = "'\"'"};
            var @endString = new Terminal(JsonTokenId.StringEndDelimiter, "EndQuote") {ErrorAlias = "'\"'"};
            var @stringCharacters = new Terminal(JsonTokenId.StringText, "Characters");
            var @escapedQuote = new Terminal(JsonTokenId.StringEscapedDelimiter, "'\\\"'");
            
            var jsonObject = new NonTerminal("Object");
            var propertyValue = new NonTerminal("PropertyValue");
            var value = new NonTerminal("Value");
            var array = new NonTerminal("Array");
            var stringValue = new NonTerminal("String");

            jsonObject.Production = @openCurly + (propertyValue + (comma + propertyValue).ZeroOrMore()).Optional() + @closeCurly;
            propertyValue.Production = stringValue + colon + value;

            value.Production = stringValue | @number | jsonObject | array | @true | @false | @null;
            array.Production = @openSquare + (value + (comma + value).ZeroOrMore()).Optional() + @closeSquare;

            stringValue.Production = @startString + (@stringCharacters | @escapedQuote).ZeroOrMore() + @endString;

            this.Root = jsonObject;
        }
    }
}
