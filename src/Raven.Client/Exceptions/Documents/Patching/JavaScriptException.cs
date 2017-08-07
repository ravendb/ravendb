using System;

namespace Raven.Client.Exceptions.Documents.Patching
{
    public class JavaScriptException : RavenException
    {
        public JavaScriptException()
        {
        }

        public JavaScriptException(string message)
            : base(message)
        {
        }

        public JavaScriptException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}