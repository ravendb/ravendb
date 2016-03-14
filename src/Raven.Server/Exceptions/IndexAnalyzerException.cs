using System;

namespace Raven.Server.Exceptions
{
    public class IndexAnalyzerException : Exception
    {
        public IndexAnalyzerException()
        {
        }

        public IndexAnalyzerException(Exception e)
            : base(e.Message, e)
        {
        }

        public IndexAnalyzerException(string message)
            : base(message)
        {
        }

        public IndexAnalyzerException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}