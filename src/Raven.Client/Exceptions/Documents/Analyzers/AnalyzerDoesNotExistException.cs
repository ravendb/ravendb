using System;

namespace Raven.Client.Exceptions.Documents.Analyzers
{
    public class AnalyzerDoesNotExistException : RavenException
    {
        public AnalyzerDoesNotExistException()
        {
        }

        public AnalyzerDoesNotExistException(string message) : base(message)
        {
        }

        public AnalyzerDoesNotExistException(string message, Exception inner) : base(message, inner)
        {
        }

        public static AnalyzerDoesNotExistException ThrowFor(string analyzerName)
        {
            throw new AnalyzerDoesNotExistException($"There is no analyzer with '{analyzerName}' name.");
        }
    }
}
