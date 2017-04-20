using System.Collections.Generic;
using Raven.Client.Documents.Replication.Messages;

namespace Raven.Server.Rachis
{
    public class ClusterNodeStatusReport
    {
        public string ClusterTag { get; set; }

        public Dictionary<string, ChangeVectorEntry[]> LastDocumentChangeVectorPerDatabase { get; set; }

        public Dictionary<string, long> LastIndexedDocumentEtagPerDatabase { get; set; }

        public Dictionary<string, ChangeVectorEntry[]> LastAttachmentChangeVectorPerDatabase { get; set; }
    }
}