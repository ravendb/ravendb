using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.Exceptions.Documents.Indexes
{
    internal class IndexCompactionInProgressException : RavenException
    {
        public IndexCompactionInProgressException()
        {
        }

        public IndexCompactionInProgressException(string message)
            : base(message)
        {
        }

        public IndexCompactionInProgressException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
