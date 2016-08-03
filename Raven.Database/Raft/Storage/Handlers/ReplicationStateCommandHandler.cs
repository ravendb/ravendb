using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Lucene.Net.Util;
using Rachis;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Replication.Data;
using Raven.Database.Raft.Commands;
using Raven.Database.Server.Tenancy;
using Raven.Json.Linq;

namespace Raven.Database.Raft.Storage.Handlers
{
    public class ReplicationStateCommandHandler : CommandHandler<ReplicationStateCommand>
    {
        public ReplicationStateCommandHandler(DocumentDatabase database, DatabasesLandlord landlord) : base(database, landlord)
        {

        }


        public override void Handle(ReplicationStateCommand command)
        {
            var key = Abstractions.Data.Constants.Cluster.ClusterReplicationStateDocumentKey;
            Database.Documents.Put(key, null, RavenJObject.FromObject(command.DatabaseToLastModified), new RavenJObject(), null);
        }
    }
}
