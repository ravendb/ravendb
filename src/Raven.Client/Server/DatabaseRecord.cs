using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Documents.Exceptions.Indexes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Transformers;
using Raven.Server.Documents.Versioning;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents
{
    public class ReplicationTopologyConfiguration
    {
        public DatabaseResolver Senator;
        public Dictionary<Guid, List<ReplicationDestination>> OutgoingConnections;
        public Dictionary<string, ScriptResolver> ResolveByCollection;
        public bool ResolveToLatest;
      
        public bool MyConnectionChanged(Guid database,ReplicationTopologyConfiguration other)
        {
            var myCurrentConnections = OutgoingConnections[database];
            var myNewConnections = OutgoingConnections[database];

            if (myCurrentConnections == null && myNewConnections == null)
                return false;
            return myCurrentConnections?.SequenceEqual(myNewConnections) ?? true;
        }

        public bool ConflictResolutionChanged(ReplicationTopologyConfiguration other)
        {
            if ((ResolveByCollection == null ^ other.ResolveByCollection == null) == false)
                return true;

            return (ResolveToLatest == other.ResolveToLatest &&
                    Senator.Equals(other.Senator) &&
                    (ResolveByCollection?.SequenceEqual(other.ResolveByCollection) ?? true)
            );
        }

//        public DynamicJsonValue ToJson()
//        {            
//            return new DynamicJsonValue
//            {
//                [nameof(Senator)] = Senator.ToJson(),
//                [nameof(OutgoingConnections)] = new DynamicJsonArray
//                {
//                    OutgoingConnections.Select(s => new DynamicJsonValue
//                    {
//                        [nameof(s.Key)] = new DynamicJsonArray(s.Value)
//                    })
//                },
//                [nameof(ResolveByCollection)] = new DynamicJsonArray
//                {
//                    ResolveByCollection.Select(r => new DynamicJsonValue
//                    {
//                        [nameof(r.Key)] = r.Value.ToJson()
//                    })
//                },
//                [nameof(ResolveToLatest)] = ResolveToLatest
//            };
//        }
    }

    public class DatabaseRecord
    {
        public DatabaseRecord()
        {
            
        }

        public DatabaseRecord(string databaseName)
        {
            DatabaseName = databaseName;
        }

        public string DatabaseName;

        public bool Disabled;

        public Dictionary<string, DeletionInProgressStatus> DeletionInProgress;

        public string DataDirectory;

        public ReplicationTopologyConfiguration ReplicationTopology;

        public DatabaseTopology Topology;

        public Dictionary<string, IndexDefinition> Indexes;

        //todo: see how we can protect this
        public Dictionary<string, TransformerDefinition> Transformers;

        public Dictionary<string, string> Settings;

        public Dictionary<string, string> SecuredSettings { get; set; }

        public VersioningConfiguration VersioningConfiguration;

        public void AddTransformer(TransformerDefinition definition)
        {
            if (Indexes != null && Indexes.ContainsKey(definition.Name))
            {
                throw new IndexOrTransformerAlreadyExistException($"Tried to create a transformer with a name of {definition.Name}, but an index under the same name exist");
            }

            TransformerDefinition existingTransformer;
            var lockMode = TransformerLockMode.Unlock;
            if (Transformers.TryGetValue(definition.Name, out existingTransformer))
            {
                if (existingTransformer.Equals(definition))
                    return;

                lockMode = existingTransformer.LockMode;
            }

            if (lockMode == TransformerLockMode.LockedIgnore)
                return;

            if (lockMode == TransformerLockMode.LockedError)
                throw new IndexOrTransformerAlreadyExistException($"Cannot edit existing transformer {definition.Name} with lock mode {lockMode}");

            Transformers[definition.Name] = definition;
        }
    }

    public enum DeletionInProgressStatus
    {
        No,
        SoftDelete,
        HardDelete
    }
}
