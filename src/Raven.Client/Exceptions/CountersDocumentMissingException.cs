using System;
using System.Collections.Generic;
using System.Text;

namespace Raven.Client.Exceptions
{
    public class CounterDocumentMissingException : System.Exception
    {
        public CounterDocumentMissingException() { }
        public CounterDocumentMissingException(string message) : base(message) { }
        public CounterDocumentMissingException(string message, System.Exception inner) : base(message, inner) { }
    }
}
