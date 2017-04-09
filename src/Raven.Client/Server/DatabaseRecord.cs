using System.Collections.Generic;
using Raven.Client.Documents.Exceptions.Indexes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Transformers;
using Raven.Server.Documents.Versioning;

namespace Raven.Client.Documents
{
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
        
        public DatabaseTopology Topology;

        public ConflictSolver ConflictSolverConfig = new ConflictSolver();

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
