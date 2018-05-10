using System;
using System.Runtime.Serialization;

namespace Raven.Server.Documents.Indexes.Static
{
    [Serializable]
    internal class JavaScriptIndexFuncException : Exception
    {
        public JavaScriptIndexFuncException()
        {
        }

        public JavaScriptIndexFuncException(string message) : base(message)
        {
        }

        public JavaScriptIndexFuncException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
