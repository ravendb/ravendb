using System;
using System.Collections.Generic;
using System.Text;

namespace Raven.Server.Exceptions
{
    public class RecursiveTypeNotSupportedException : Exception
    {
        public RecursiveTypeNotSupportedException(string message) : base(message)
        {
        }
    }
}
