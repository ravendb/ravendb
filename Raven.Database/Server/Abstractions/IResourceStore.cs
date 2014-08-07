using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Database.Server.Connections;

namespace Raven.Database.Server.Abstractions
{
    public interface IResourceStore
    {
        string Name { get; }
        TransportState TransportState {get ; }
    }
}
