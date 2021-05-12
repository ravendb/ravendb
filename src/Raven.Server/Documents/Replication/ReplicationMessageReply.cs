using Raven.Client.Documents.Operations.Replication;

namespace Raven.Server.Documents.Replication
{
    internal class ReplicationMessageReply
    {
        internal enum ReplyType
        {
            None,
            Ok,
            Error, 
            MissingAttachments
        }

        public ReplyType Type { get; set; }
        public long LastEtagAccepted { get; set; }
        public string Exception { get; set; }
        public string Message { get; set; }
        public string MessageType { get; set; }
        public string DatabaseChangeVector { get; set; }
        
        public string[] AcceptablePaths { get; set; }

        public PreventDeletionsMode PreventDeletionsMode { get; set; }
        public string DatabaseId { get; set; }
        public string NodeTag { get; set; }
        public long CurrentEtag { get; set; }
    }
}
