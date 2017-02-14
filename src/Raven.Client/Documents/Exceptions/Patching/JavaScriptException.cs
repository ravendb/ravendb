using System;
using Raven.Client.Exceptions;

namespace Raven.Client.Documents.Exceptions.Patching
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