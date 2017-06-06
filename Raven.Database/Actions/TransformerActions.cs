// -----------------------------------------------------------------------
//  <copyright file="TransformerActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Database.Impl;
using Raven.Database.Storage;
using Raven.Json.Linq;

namespace Raven.Database.Actions
{
    public class TransformerActions : ActionsBase
    {
        /// <summary>
        /// For temporary transformers we assign negative indexes
        /// </summary>
        private int temporaryTransfomerIndex = -1;

        public TransformerActions(DocumentDatabase database, IUuidGenerator uuidGenerator, ILog log)
            : base(database, uuidGenerator, log)
        {
        }

        internal string[] Names
        {
            get { return IndexDefinitionStorage.TransformerNames; }
        }

        internal TransformerDefinition[] Definitions
        {
            get { return IndexDefinitionStorage.GetAllTransformerDefinitions().ToArray(); }
        }

        public RavenJArray GetTransformerNames(int start, int pageSize)
        {
            return new RavenJArray(
                IndexDefinitionStorage.TransformerNames.Skip(start).Take(pageSize)
                    .Select(s => new RavenJValue(s))
                );
        }

        public RavenJArray GetTransformers(int start, int pageSize)
        {
            return new RavenJArray(
            IndexDefinitionStorage.TransformerNames.Skip(start).Take(pageSize)
                .Select(
                    indexName =>
                    {
                        var definitions = IndexDefinitionStorage.GetTransformerDefinition(indexName);
                        return new RavenJObject
                                     {
                                         {"name", new RavenJValue(indexName) },
                                         {"definition", RavenJObject.FromObject(definitions)},
                                         {"lockMode", new RavenJValue(definitions.LockMode.ToString())}
                                     };
                    }));

        }

        public TransformerDefinition GetTransformerDefinition(string name)
        {
            return IndexDefinitionStorage.GetTransformerDefinition(name);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool DeleteTransform(string name, int? deletedTransformerVersion)
        {
            var transformer = GetTransformerDefinition(name);
            if (transformer == null)
                return false;

            var currentTransformerVersion = transformer.TransformerVersion;
            if (deletedTransformerVersion != null &&
                currentTransformerVersion != null &&
                currentTransformerVersion > deletedTransformerVersion)
            {
                // the transformer version is larger than the deleted one
                // got an old delete from an outdated node
                return false;
            }

            if (IndexDefinitionStorage.RemoveTransformer(name) == false) 
                return false;

            var version = currentTransformerVersion ?? 0;
            if (deletedTransformerVersion != null)
                version = Math.Max(version, deletedTransformerVersion.Value);

            TransactionalStorage.Batch(actions =>
            {
                var metadata = new RavenJObject
                {
                    {IndexDefinitionStorage.TransformerVersionKey, version }
                };
                actions.Lists.Set(Constants.RavenDeletedTransformersVersions, name, metadata, UuidType.Transformers);
            });

            //raise notification only if the transformer was actually removed
            TransactionalStorage.ExecuteImmediatelyOrRegisterForSynchronization(() => Database.Notifications.RaiseNotifications(new TransformerChangeNotification
            {
                Name = name,
                Type = TransformerChangeTypes.TransformerRemoved,
                Version = version
            }));

            return true;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public string PutTransform(string name, TransformerDefinition definition, bool isReplication = false)
        {
            if (name == null) throw new ArgumentNullException("name");
            if (definition == null) throw new ArgumentNullException("definition");

            name = name.Trim();

            var deletedVersion = GetDeletedTransformerVersion(Constants.RavenDeletedTransformersVersions, name);
            var tombstoneVersion = GetDeletedTransformerVersion(Constants.RavenReplicationTransformerTombstones, name);
            var deletedTransformerVersion = Math.Max(deletedVersion, tombstoneVersion);
            if (isReplication && definition.TransformerVersion != null &&
                deletedTransformerVersion > definition.TransformerVersion)
            {
                // no op for an outdated transformer
                return name;
            }

            var existingDefinition = IndexDefinitionStorage.GetTransformerDefinition(name);
            if (existingDefinition != null)
            {
                var newTransformerVersion = definition.TransformerVersion;
                var currentTransformerVersion = existingDefinition.TransformerVersion;

                // whether we update the transformer definition or not,
                // we need to update the transformer version
                existingDefinition.TransformerVersion = definition.TransformerVersion =
                    Math.Max(currentTransformerVersion ?? 0, newTransformerVersion ?? 0);

                switch (isReplication)
                {
                    case true:
                        if (newTransformerVersion != null && currentTransformerVersion != null &&
                            newTransformerVersion <= currentTransformerVersion)
                        {
                            //this new transformer is an older version of the current one
                            return null;
                        }

                        // we need to update the lock mode only if it was updated by another server
                        existingDefinition.LockMode = definition.LockMode;
                        break;
                    default:
                        switch (existingDefinition.LockMode)
                        {
                            case TransformerLockMode.Unlock:
                                if (existingDefinition.Equals(definition))
                                    return name; // no op for the same transformer

                                definition.TransformerVersion++;

                                break;
                            case TransformerLockMode.LockedIgnore:
                                Log.Info("Transformer {0} not saved because it was lock (with ignore)", name);
                                return name;
                            default:
                                throw new ArgumentOutOfRangeException();
                        }
                        break;
                }
            }
            else if (isReplication == false)
            {
                // we're creating a new transformer,
                // we need to take the transformer version of the deleted transformer (if exists)
                definition.TransformerVersion = Math.Max(definition.TransformerVersion ?? 0, deletedTransformerVersion) + 1;
            }

            if (definition.TransformerVersion == null)
                definition.TransformerVersion = 0;

            var generator = IndexDefinitionStorage.CompileTransform(definition);

            if (existingDefinition != null)
                IndexDefinitionStorage.RemoveTransformer(existingDefinition.TransfomerId);

            var temporary = definition.Temporary;

            if (temporary)
            {
                definition.TransfomerId = Database.Transformers.GetNextTemporaryTransformerIndex();
                IndexDefinitionStorage.CreateTransform(definition, generator);
                IndexDefinitionStorage.AddTransform(definition.TransfomerId, definition);
            }
            else
            {
                TransactionalStorage.Batch(accessor =>
                {
                    definition.TransfomerId = (int)Database.Documents.GetNextIdentityValueWithoutOverwritingOnExistingDocuments("TransformerId", accessor);
                });

                IndexDefinitionStorage.CreateTransform(definition, generator);
                IndexDefinitionStorage.PersistTransform(definition);
                IndexDefinitionStorage.AddTransform(definition.TransfomerId, definition);

                TransactionalStorage.ExecuteImmediatelyOrRegisterForSynchronization(() => Database.Notifications.RaiseNotifications(new TransformerChangeNotification()
                {
                    Name = name,
                    Type = TransformerChangeTypes.TransformerAdded,
                    Version = definition.TransformerVersion
                }));
            }

            return name;
        }

        private int GetDeletedTransformerVersion(string listName, string transformerName)
        {
            var version = 0;

            TransactionalStorage.Batch(action =>
            {
                var li = action.Lists.Read(listName, transformerName);
                if (li == null)
                    return;

                var versionStr = li.Data.Value<string>("TransformerVersion");
                int.TryParse(versionStr, out version);
            });

            return version;
        }

        public int GetNextTemporaryTransformerIndex()
        {
            return Interlocked.Decrement(ref temporaryTransfomerIndex);
        }
    }
}
