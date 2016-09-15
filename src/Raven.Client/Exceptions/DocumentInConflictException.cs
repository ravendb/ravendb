using System;

namespace Raven.Client.Exceptions
{
    public class DocumentInConflictException : Exception
    {
        public DocumentInConflictException(string msg) : base(msg)
        {

        }
    }
}
