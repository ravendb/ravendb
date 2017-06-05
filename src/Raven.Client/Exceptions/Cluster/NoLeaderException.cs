using System;
using System.Collections.Generic;
using System.Text;

namespace Raven.Client.Exceptions.Cluster
{
    public class NoLeaderException : RavenException
    {
        public NoLeaderException()
        {
        }

        public NoLeaderException(string message) : base(message)
        {
        }

        public NoLeaderException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
