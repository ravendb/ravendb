using System;
using Raven.Client.Exceptions.Compilation;
using Raven.Client.Extensions;

namespace Raven.Client.Exceptions.Documents.Compilation
{
    public class SorterCompilationException : CompilationException
    {
        public SorterCompilationException()
        {
        }

        public SorterCompilationException(string message)
            : base(message)
        {
        }

        public SorterCompilationException(string message, Exception inner)
            : base(message, inner)
        {
        }

        public override string ToString()
        {
            return this.ExceptionToString(null);
        }
    }
}
