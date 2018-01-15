using System;
using System.Collections.Generic;
using System.Text;

namespace Raven.Server.Exceptions
{
    public class RecursiveTypeNotSupported: Exception
    {
        public RecursiveTypeNotSupported(string message) : base(message)
        {
            
        }
    }
}
