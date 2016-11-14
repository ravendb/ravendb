using System;
using System.Runtime.Serialization;
using Raven.NewClient.Abstractions.Extensions;

namespace Raven.NewClient.Abstractions.Exceptions
{
    public class TransformCompilationException : Exception
    {
        //
        // For guidelines regarding the creation of new exception types, see
        //    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/cpgenref/html/cpconerrorraisinghandlingguidelines.asp
        // and
        //    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/dncscol/html/csharp07192001.asp
        //

        public TransformCompilationException()
        {
        }

        public TransformCompilationException(string message) : base(message)
        {
        }

        public TransformCompilationException(string message, Exception inner) : base(message, inner)
        {
        }

    }
}
