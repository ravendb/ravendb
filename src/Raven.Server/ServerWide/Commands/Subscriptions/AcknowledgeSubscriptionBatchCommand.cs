using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents;
using Raven.Client.Documents.Replication.Messages;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public class AcknowledgeSubscriptionBatchCommand:UpdateDatabaseCommand
    {
        public ChangeVectorEntry[] ChangeVector;
        public long SubscriptionEtag;
        public string NodeTag;

        // for serializtion
        private AcknowledgeSubscriptionBatchCommand() : base(null){}

        public AcknowledgeSubscriptionBatchCommand(string databaseName) : base(databaseName)
        {
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.AcknowledgeSubscriptionBatch(SubscriptionEtag, ChangeVector,NodeTag);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(ChangeVector)] = ChangeVector?.ToJson();
            json[nameof(SubscriptionEtag)] = SubscriptionEtag;
            json[nameof(NodeTag)] = NodeTag;
        }
    }
}
