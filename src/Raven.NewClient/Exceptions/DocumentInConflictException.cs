using System;

namespace Raven.NewClient.Client.Exceptions
{
    public class DocumentInConflictException : Exception
    {
        public DocumentInConflictException(string msg) : base(msg)
        {

        }
    }
}
