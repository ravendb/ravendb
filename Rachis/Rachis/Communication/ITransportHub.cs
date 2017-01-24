using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Rachis.Communication
{
    public interface ITransportHub
    {
        /// <summary>
        /// This entity is in charge of establishing communication between nodes.
        /// </summary>
        /// <param name="nodeInfo"> Node information needed to establish connection to it.</param>
        /// <returns>The establish connection bus.</returns>
        ITransportBus ConnectToNode(NodeConnectionInfo nodeInfo);
    }
}
