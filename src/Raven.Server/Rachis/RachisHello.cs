using Raven.Client.Server.Tcp;

namespace Raven.Server.Rachis
{
    /// <summary>
    /// Initial message sent when we open a connection to a remote server
    /// </summary>
    public class RachisHello
    {
        /// <summary>
        /// Used to filter messages from a server that connected to us by mistake
        /// </summary>
        public string TopologyId;
        /// <summary>
        /// Debug string that we use to identify the source, meant to be human readable
        /// </summary>
        public string DebugSourceIdentifier;
        /// <summary>
        /// Debug string that we use to identify the destination, meant to be human readable
        /// </summary>
        public string DebugDestinationIdentifier;
        /// <summary>
        /// The purpose of this communication
        /// </summary>
        public InitialMessageType InitialMessageType;

    }
}