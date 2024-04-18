using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace Corax
{
    [Serializable]
    internal class IndexOpenException : Exception
    {
        public IndexOpenException()
        {
        }

        public IndexOpenException(string message) : base(message)
        {
        }

        public IndexOpenException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
