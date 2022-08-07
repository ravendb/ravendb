using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Server.Exceptions
{
    public class OutOfDiskSpaceException : Exception
    {
        public OutOfDiskSpaceException(Exception e)
            : base(e.Message, e)
        {
        }

        public OutOfDiskSpaceException(string message, Exception e)
            : base(message, e)
        {
        }
    }
}
