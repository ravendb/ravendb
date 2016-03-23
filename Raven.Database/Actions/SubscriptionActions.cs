// -----------------------------------------------------------------------
//  <copyright file="SubscriptionActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Impl;
using Raven.Database.Util;
using Raven.Json.Linq;
using Sparrow.Collections;

namespace Raven.Database.Actions
{
    public class SubscriptionActions : ActionsBase
    {
        private readonly ConcurrentDictionary<long, SubscriptionConnectionOptions> openSubscriptions = 
            new ConcurrentDictionary<long, SubscriptionConnectionOptions>();

        private readonly ConcurrentDictionary<long, PutSerialLock> locks = new ConcurrentDictionary<long, PutSerialLock>();

        private readonly ConcurrentDictionary<long, SizeLimitedConcurrentSet<string>> forciblyReleasedSubscriptions = new ConcurrentDictionary<long, SizeLimitedConcurrentSet<string>>();

        public SubscriptionActions(DocumentDatabase database, ILog log)
            : base(database, null, null, log)
        {
        }

        public long CreateSubscription(SubscriptionCriteria criteria)
        {
            long id = -1;

            Database.TransactionalStorage.Batch(accessor =>
            {
                id = accessor.General.GetNextIdentityValue(Constants.RavenSubscriptionsPrefix);

                var config = new SubscriptionConfig
                {
                    SubscriptionId = id,
                    Criteria = criteria,
                    AckEtag = criteria.StartEtag ?? Etag.Empty,
                };

                SaveSubscriptionConfig(id, config);
            });

            return id;
        }

        private IDisposable LockSubscription(long  id)
        {
            return locks.GetOrAdd(id, new PutSerialLock()).Lock();
        }

        public void DeleteSubscription(long id)
        {
            using (LockSubscription(id))
            {
                Database.TransactionalStorage.Batch(accessor => accessor.Lists.Remove(Constants.RavenSubscriptionsPrefix, id.ToString("D19")));
            }
        }

        private void SaveSubscriptionConfig(long id, SubscriptionConfig config)
        {
            Database.TransactionalStorage.Batch(accessor => 
                accessor.Lists.Set(Constants.RavenSubscriptionsPrefix, id.ToString("D19"), RavenJObject.FromObject(config), UuidType.Subscriptions));
        }

        public void OpenSubscription(long id, SubscriptionConnectionOptions options)
        {
            SizeLimitedConcurrentSet<string> releasedConnections;
            if (forciblyReleasedSubscriptions.TryGetValue(id, out releasedConnections) && releasedConnections.Contains(options.ConnectionId))
                throw new SubscriptionClosedException("Subscription " + id + " was forcibly released. Cannot reopen it.");

            if (openSubscriptions.TryAdd(id, options))
            {
                UpdateClientActivityDate(id);
                return;
            }

            SubscriptionConnectionOptions existingOptions;

            if(openSubscriptions.TryGetValue(id, out existingOptions) == false)
                throw new SubscriptionDoesNotExistException("Didn't get existing open subscription while it's expected. Subscription id: " + id);

            if (existingOptions.ConnectionId.Equals(options.ConnectionId, StringComparison.OrdinalIgnoreCase))
            {
                // reopen subscription on already existing connection - might happen after network connection problems the client tries to reopen
                UpdateClientActivityDate(id);
                return; 
            }

            var config = GetSubscriptionConfig(id);

            var now = SystemTime.UtcNow;
            var timeSinceBatchSent = now - config.TimeOfSendingLastBatch;

            if (timeSinceBatchSent > existingOptions.BatchOptions.AcknowledgmentTimeout && 
                SystemTime.UtcNow - config.TimeOfLastClientActivity > TimeSpan.FromTicks(existingOptions.ClientAliveNotificationInterval.Ticks * 3))
            {
                // last connected client exceeded ACK timeout and didn't send at least two 'client-alive' notifications - let the requesting client to open it
                ForceReleaseAndOpenForNewClient(id, options);
                return;
            }

            switch (options.Strategy)
            {
                case SubscriptionOpeningStrategy.TakeOver:
                    if (existingOptions.Strategy != SubscriptionOpeningStrategy.ForceAndKeep)
                    {
                        ForceReleaseAndOpenForNewClient(id, options);
                        return;
                    }
                    break;
                case SubscriptionOpeningStrategy.ForceAndKeep:
                    ForceReleaseAndOpenForNewClient(id, options);
                    return;
            }

            throw new SubscriptionInUseException("Subscription is already in use. There can be only a single open subscription connection per subscription.");
        }

        private void ForceReleaseAndOpenForNewClient(long id, SubscriptionConnectionOptions options)
        {
            ReleaseSubscription(id);
            openSubscriptions.TryAdd(id, options);
            UpdateClientActivityDate(id);
        }

        public void ReleaseSubscription(long id, bool forced = false)
        {
            SubscriptionConnectionOptions options;
            openSubscriptions.TryRemove(id, out options);

            if (forced && options != null)
            {
                forciblyReleasedSubscriptions.GetOrAdd(id, new SizeLimitedConcurrentSet<string>(50, StringComparer.OrdinalIgnoreCase)).Add(options.ConnectionId);
            }
        }

        public void AcknowledgeBatchProcessed(long id, Etag lastEtag)
        {
            using (LockSubscription(id))
            {
                TransactionalStorage.Batch(accessor =>
                {
                    var config = GetSubscriptionConfig(id);
                    var options = GetBatchOptions(id);

                    var timeSinceBatchSent = SystemTime.UtcNow - config.TimeOfSendingLastBatch;
                    if (timeSinceBatchSent > options.AcknowledgmentTimeout)
                        throw new TimeoutException("The subscription cannot be acknowledged because the timeout has been reached.");


                    if (config.AckEtag.CompareTo(lastEtag) < 0)
                        config.AckEtag = lastEtag;

                    config.TimeOfLastClientActivity = SystemTime.UtcNow;

                    SaveSubscriptionConfig(id, config);
                });
            }
        }

        public void AssertOpenSubscriptionConnection(long id, string connection)
        {
            SubscriptionConnectionOptions options;
            if (openSubscriptions.TryGetValue(id, out options) == false)
                throw new SubscriptionClosedException("There is no subscription with id: " + id + " being opened");

            if (options.ConnectionId.Equals(connection, StringComparison.OrdinalIgnoreCase) == false)
            {
                // prevent from concurrent work of multiple clients against the same subscription
                throw new SubscriptionInUseException("Subscription is being opened for a different connection.");
            }
        }

        public SubscriptionBatchOptions GetBatchOptions(long id)
        {
            SubscriptionConnectionOptions options;
            if (openSubscriptions.TryGetValue(id, out options) == false)
                throw new SubscriptionClosedException("There is no open subscription with id: " + id);

            return options.BatchOptions;
        }

        public SubscriptionConfig GetSubscriptionConfig(long id)
        {
            SubscriptionConfig config = null;

            TransactionalStorage.Batch(accessor =>
            {
                var listItem = accessor.Lists.Read(Constants.RavenSubscriptionsPrefix, id.ToString("D19"));

                if(listItem == null)
                    throw new SubscriptionDoesNotExistException("There is no subscription configuration for specified identifier (id: " + id + ")");

                config = listItem.Data.JsonDeserialization<SubscriptionConfig>();
            });

            return config;
        }

        public void UpdateBatchSentTime(long id)
        {
            using (LockSubscription(id))
            {
                TransactionalStorage.Batch(accessor =>
                {
                    var config = GetSubscriptionConfig(id);

                    config.TimeOfSendingLastBatch = SystemTime.UtcNow;
                    config.TimeOfLastClientActivity = SystemTime.UtcNow;

                    SaveSubscriptionConfig(id, config);
                });
            }
        }

        public void UpdateClientActivityDate(long id)
        {
            using (LockSubscription(id))
            {
                TransactionalStorage.Batch(accessor =>
                {
                    var config = GetSubscriptionConfig(id);

                    config.TimeOfLastClientActivity = SystemTime.UtcNow;

                    SaveSubscriptionConfig(id, config);
                });
            }
        }

        public List<SubscriptionConfig> GetSubscriptions(int start, int take)
        {
            var subscriptions = new List<SubscriptionConfig>();

            TransactionalStorage.Batch(accessor =>
            {
                foreach (var listItem in accessor.Lists.Read(Constants.RavenSubscriptionsPrefix, start, take))
                {
                    var config = listItem.Data.JsonDeserialization<SubscriptionConfig>();
                    subscriptions.Add(config);
                }
            });

            return subscriptions;
        }

        public class SubscriptionDebugInfo : SubscriptionConfig
        {
            public bool IsOpen { get; set; }
            public SubscriptionConnectionOptions ConnectionOptions { get; set; }
        }

        public List<object> GetDebugInfo()
        {
            var subscriptions = new List<object>();

            TransactionalStorage.Batch(accessor =>
            {
                foreach (var listItem in accessor.Lists.Read(Constants.RavenSubscriptionsPrefix, 0, int.MaxValue))
                {
                    var config = listItem.Data.JsonDeserialization<SubscriptionConfig>();

                    SubscriptionConnectionOptions options = null;
                    openSubscriptions.TryGetValue(config.SubscriptionId, out options);

                    var debugInfo = new
                    {
                        config.SubscriptionId,
                        config.AckEtag,
                        TimeOfLastClientActivity = config.TimeOfLastClientActivity != default(DateTime) ? config.TimeOfLastClientActivity : (DateTime?)null,
                        TimeOfSendingLastBatch = config.TimeOfSendingLastBatch != default(DateTime) ? config.TimeOfSendingLastBatch : (DateTime?)null,
                        Criteria = new
                        {
                            config.Criteria.KeyStartsWith,
                            config.Criteria.BelongsToAnyCollection,
                            PropertiesMatch = config.Criteria.PropertiesMatch == null ? null : config.Criteria.PropertiesMatch.Select(x => new
                            {
                                x.Key,x.Value
                            }).ToList(),
                            PropertiesNotMatch = config.Criteria.PropertiesNotMatch == null ? null : config.Criteria.PropertiesNotMatch.Select(x => new
                            {
                                x.Key, x.Value
                            }).ToList(),
                            config.Criteria.StartEtag
                        },
                        IsOpen = options != null,
                        ConnectionOptions = options
                    };

                    
                    
                    subscriptions.Add(debugInfo);
                }
            });

            return subscriptions;
        }

        public void SetAcknowledgeEtag(long id, Etag lastEtag)
        {
            TransactionalStorage.Batch(accessor =>
            {
                var config = GetSubscriptionConfig(id);
                config.AckEtag = lastEtag;
                SaveSubscriptionConfig(id, config);
            });
        }

        public Etag GetAcknowledgeEtag(long id)
        {
            Etag etag = null;
            TransactionalStorage.Batch(accessor =>
            {
                etag = GetSubscriptionConfig(id).AckEtag;
            });
            return etag;
        }
    }
}
