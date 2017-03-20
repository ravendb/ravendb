using System;

namespace Raven.Client.Exceptions.Compilation
{
    public abstract class CompilationException : RavenException
    {
        protected CompilationException()
        {
        }

        protected CompilationException(string message)
            : base(message)
        {
        }

        protected CompilationException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}