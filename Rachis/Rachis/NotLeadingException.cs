using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace Rachis
{
    public class NotLeadingException : Exception
    {
        public string CurrentLeader { get; set; }

        public NotLeadingException()
        {
        }

        public NotLeadingException(string message) : base(message)
        {
        }

        public NotLeadingException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
