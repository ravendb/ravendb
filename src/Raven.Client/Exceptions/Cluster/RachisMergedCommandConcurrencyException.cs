using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.Exceptions.Cluster
{
    public sealed class RachisMergedCommandConcurrencyException : RavenException
    {
        public RachisMergedCommandConcurrencyException(string message) : base(message)
        {
        }
    }
}
