namespace Rachis.Messages
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
        /// The purpose of this communication
        /// </summary>
        public InitialMessageType InitialMessageType;
    }


    public enum InitialMessageType
    {
        RequestVote,
        AppendEntries
    }
}