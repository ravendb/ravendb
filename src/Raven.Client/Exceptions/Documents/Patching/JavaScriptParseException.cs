using System;

namespace Raven.Client.Exceptions.Documents.Patching
{
    public class JavaScriptParseException : JavaScriptException
    {
        public JavaScriptParseException()
        {
        }

        public JavaScriptParseException(string message)
            : base(message)
        {
        }

        public JavaScriptParseException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}