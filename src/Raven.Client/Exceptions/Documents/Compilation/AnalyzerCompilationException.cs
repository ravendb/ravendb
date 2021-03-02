using System;
using Raven.Client.Exceptions.Compilation;
using Raven.Client.Extensions;

namespace Raven.Client.Exceptions.Documents.Compilation
{
    public class AnalyzerCompilationException : CompilationException
    {
        public AnalyzerCompilationException()
        {
        }

        public AnalyzerCompilationException(string message)
            : base(message)
        {
        }

        public AnalyzerCompilationException(string message, Exception inner)
            : base(message, inner)
        {
        }

        public override string ToString()
        {
            return this.ExceptionToString(null);
        }
    }
}
