using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rachis;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Replication.Data;
using Raven.Database.Raft.Commands;
using Raven.Database.Server.Tenancy;

namespace Raven.Database.Raft.Storage.Handlers
{
    public class ReplicationStateCommandHandler : CommandHandler<ReplicationStateCommand>
    {
        public ReplicationStateCommandHandler(DocumentDatabase database, DatabasesLandlord landlord,RaftEngine engine) : base(database, landlord)
        {
            MaxReplicationLatency = database.Configuration.Cluster.MaxReplicationLatency;
            Engine = engine;
        }

        private RaftEngine Engine { get; set; }

        //default should be 2 minutes
        public TimeSpan MaxReplicationLatency { get; set; }

        public override void Handle(ReplicationStateCommand command)
        {
            //If we are the leader we won't have documents sent from ourself so it is a noop.
            if (Engine.State == RaftEngineState.Leader)
                return;
            var lastChanges = command.DatabaseToLastModified;
            foreach (var databaseName in lastChanges.Keys)
            {
                Task<DocumentDatabase> databaseTask;
                if(Landlord.TryGetOrCreateResourceStore(databaseName, out databaseTask) == false)
                    throw new InvalidOperationException($"ReplicationStateCommand could not load database: {databaseName}");
                var database = databaseTask.Result;
                var lastModify = lastChanges[databaseName];
                var docKey = $"{Abstractions.Data.Constants.RavenReplicationSourcesBasePath}/{lastModify.Item2}";
                var doc = database.Documents.Get(docKey, null);
                if (doc == null)
                    throw new InvalidOperationException($"Missing replication source for database {database.Name}");
                var sourceInformation = doc.DataAsJson.JsonDeserialization<SourceReplicationInformation>();
                var lastUpdate = sourceInformation.LastModifiedAtSource ?? DateTime.MinValue;
                if (lastUpdate + MaxReplicationLatency < lastModify.Item1)
                    throw new InvalidOperationException($"Source document for database: {database.Name} " +
                                             $"was last updated at {lastUpdate}, refusing to append replication state");
            }   
            // we are up to date with the leader (up to replication latency).        
        }
    }
}
