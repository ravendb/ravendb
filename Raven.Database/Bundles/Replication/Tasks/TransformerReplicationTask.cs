// -----------------------------------------------------------------------
//  <copyright file="TransformerReplicationTask.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Bundles.Replication.Impl;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.Replication.Tasks
{
    public class TransformerReplicationTask : ReplicationTaskBase
    {
        private readonly static ILog Log = LogManager.GetCurrentClassLogger();

        private readonly TimeSpan replicationFrequency;
        private Timer timer;
        private readonly object replicationLock = new object();

        public TransformerReplicationTask(DocumentDatabase database, HttpRavenRequestFactory httpRavenRequestFactory, ReplicationTask replication)
            : base(database, httpRavenRequestFactory, replication)
        {
            replicationFrequency = TimeSpan.FromSeconds(database.Configuration.IndexAndTransformerReplicationLatencyInSec); //by default 10 min
            TimeToWaitBeforeSendingDeletesOfTransformersToSiblings = TimeSpan.FromMinutes(1);
        }

        public TimeSpan TimeToWaitBeforeSendingDeletesOfTransformersToSiblings { get; set; }

        public void Start()
        {
            Database.Notifications.OnTransformerChange += OnTransformerChange;

            timer = Database.TimerManager.NewTimer(x => Execute(), TimeSpan.Zero, replicationFrequency);
        }

        private void OnTransformerChange(DocumentDatabase documentDatabase, TransformerChangeNotification notification)
        {
            var transformerName = notification.Name;
            switch (notification.Type)
            {
                case TransformerChangeTypes.TransformerAdded:
                    //if created transformer with the same name as deleted one, we should prevent its deletion replication
                    Database.TransactionalStorage.Batch(accessor => 
                        accessor.Lists.Remove(Constants.RavenReplicationTransformerTombstones, transformerName));

                    break;
                case TransformerChangeTypes.TransformerRemoved:
                    //If we don't have any destination to replicate to (we are probably slave node)
                    //we shouldn't keep a tombstone since we are not going to remove it anytime
                    //and keeping it prevents us from getting that transformer created again.
                    if (GetReplicationDestinations().Count == 0)
                        return;

                    var metadata = new RavenJObject
                    {
                        {Constants.RavenTransformerDeleteMarker, true},
                        {Constants.RavenReplicationSource, Database.TransactionalStorage.Id.ToString()},
                        {Constants.RavenReplicationVersion, ReplicationHiLo.NextId(Database)},
                        {IndexDefinitionStorage.TransformerVersionKey, notification.Version }
                    };

                    Database.TransactionalStorage.Batch(accessor => 
                        accessor.Lists.Set(Constants.RavenReplicationTransformerTombstones, transformerName, metadata, UuidType.Transformers));
                    break;
            }
        }

        public bool Execute(Func<ReplicationDestination, bool> shouldSkipDestinationPredicate = null,
            bool forceTombstoneReplication = false)
        {
            if (Database.Disposed)
                return false;

            if (Monitor.TryEnter(replicationLock) == false)
                return false;

            try
            {
                using (CultureHelper.EnsureInvariantCulture())
                {
                    shouldSkipDestinationPredicate = shouldSkipDestinationPredicate ?? (x => x.SkipIndexReplication == false);
                    var replicationDestinations = GetReplicationDestinations(x => shouldSkipDestinationPredicate(x));
                    var replicatedTransformerTombstones = new Dictionary<string, int>();

                    foreach (var destination in replicationDestinations)
                    {
                        var now = SystemTime.UtcNow;

                        var transformerTombstones = GetTombstones(Constants.RavenReplicationTransformerTombstones, 0, 64,
                            // we don't send out deletions immediately, we wait for a bit
                            // to make sure that the user didn't reset the index or delete / create
                            // things manually
                            x => forceTombstoneReplication || (now - x.CreatedAt) >= TimeToWaitBeforeSendingDeletesOfTransformersToSiblings);

                        try
                        {
                            ReplicateTransformerDeletionIfNeeded(transformerTombstones, destination, replicatedTransformerTombstones);

                            if (Database.Transformers.Definitions.Length > 0)
                            {
                                foreach (var definition in Database.Transformers.Definitions)
                                    ReplicateSingleTransformer(destination, definition);
                            }
                        }
                        catch (Exception e)
                        {
                            Log.ErrorException("Failed to replicate transformers to " + destination, e);
                        }
                    }

                    Database.TransactionalStorage.Batch(actions =>
                    {
                        foreach (var transformerTombstone in replicatedTransformerTombstones)
                        {
                            var transformerExists = Database.Transformers.GetTransformerDefinition(transformerTombstone.Key) != null;
                            if (transformerTombstone.Value != replicationDestinations.Count &&
                                transformerExists == false)
                                continue;

                            actions.Lists.Remove(Constants.RavenReplicationTransformerTombstones, transformerTombstone.Key);
                        }
                    });

                    return true;
                }
            }
            catch (Exception e)
            {
                Log.ErrorException("Failed to replicate transformers", e);

                return false;
            }
            finally
            {
                Monitor.Exit(replicationLock);
            }
        }

        public void Execute(string transformerName)
        {
            var definition = Database.IndexDefinitionStorage.GetTransformerDefinition(transformerName);

            if (definition == null)
                return;

            foreach (var destination in GetReplicationDestinations(x => x.SkipIndexReplication == false))
            {
                ReplicateSingleTransformer(destination, definition);
            }
        }

        private void ReplicateSingleTransformer(ReplicationStrategy destination, TransformerDefinition definition)
        {
            try
            {
                var clonedTransformer = definition.Clone();
                clonedTransformer.TransfomerId = 0;

                var url = destination.ConnectionStringOptions.Url + "/transformers/" + Uri.EscapeUriString(definition.Name) + "?" + GetDebugInformation();
                var replicationRequest = HttpRavenRequestFactory.Create(url, HttpMethod.Put, destination.ConnectionStringOptions, Replication.GetRequestBuffering(destination));
                replicationRequest.Write(RavenJObject.FromObject(clonedTransformer));
                replicationRequest.ExecuteRequest();
            }
            catch (Exception e)
            {
                Replication.HandleRequestBufferingErrors(e, destination);

                Log.WarnException("Could not replicate transformer " + definition.Name + " to " + destination.ConnectionStringOptions.Url, e);
            }
        }

        private void ReplicateTransformerDeletionIfNeeded(List<JsonDocument> transformerTombstones, ReplicationStrategy destination, Dictionary<string, int> replicatedTransformerTombstones)
        {
            if (transformerTombstones.Count == 0)
                return;

            foreach (var tombstone in transformerTombstones)
            {
                try
                {
                    int value;
                    if (Database.Transformers.GetTransformerDefinition(tombstone.Key) != null) //if in the meantime the transformer was recreated under the same name
                    {
                        replicatedTransformerTombstones.TryGetValue(tombstone.Key, out value);
                        replicatedTransformerTombstones[tombstone.Key] = value + 1;
                        continue;
                    }

                    var url = string.Format("{0}/transformers/{1}?{2}&{3}", 
                        destination.ConnectionStringOptions.Url, 
                        Uri.EscapeUriString(tombstone.Key),
                        GetTombstoneVersion(tombstone, IndexDefinitionStorage.TransformerVersionKey, Constants.TransformerVersion),
                        GetDebugInformation());
                    var replicationRequest = HttpRavenRequestFactory.Create(url, HttpMethods.Delete, destination.ConnectionStringOptions, Replication.GetRequestBuffering(destination));
                    replicationRequest.Write(RavenJObject.FromObject(EmptyRequestBody));
                    replicationRequest.ExecuteRequest();
                    Log.Info("Replicated transformer deletion (transformer name = {0})", tombstone.Key);
                    replicatedTransformerTombstones.TryGetValue(tombstone.Key, out value);
                    replicatedTransformerTombstones[tombstone.Key] = value + 1;
                }
                catch (Exception e)
                {
                    Replication.HandleRequestBufferingErrors(e, destination);

                    Log.ErrorException(string.Format("Failed to replicate transformer deletion (transformer name = {0})", tombstone.Key), e);
                }
            }
        }

        public override void Dispose()
        {
            if (timer != null)
                timer.Dispose();
        }
    }
}
