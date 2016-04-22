using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Counters.Notifications;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Common;
using Raven.Database.Config;
using Raven.Database.Counters.Controllers;
using Raven.Database.Counters.Notifications;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Server.Connections;
using Raven.Database.Util;
using Raven.Imports.Newtonsoft.Json;
using Voron;
using Voron.Impl;
using Voron.Trees;
using Voron.Util;
using Voron.Util.Conversion;
using Constants = Raven.Abstractions.Data.Constants;

namespace Raven.Database.Counters
{
    public class CounterStorage : IResourceStore, IDisposable
    {
        private static readonly ILog Log = LogManager.GetCurrentClassLogger();

        private readonly StorageEnvironment storageEnvironment;
        private readonly TransportState transportState;
        private readonly CountersMetricsManager metricsCounters;
        private readonly NotificationPublisher notificationPublisher;
        private readonly ReplicationTask replicationTask;
        private readonly JsonSerializer jsonSerializer;
        private readonly Guid tombstoneId = Guid.Empty;
        private readonly int sizeOfGuid;
        private readonly TimeSpan tombstoneRetentionTime;
        private readonly int deletedTombstonesInBatch;

        private long lastEtag;
        private long lastCounterId;
        private Timer purgeTombstonesTimer;
        public event Action CounterUpdated = () => { };

        public string CounterStorageUrl { get; private set; }

        public DateTime LastWrite { get; private set; }

        public Guid ServerId { get; private set; }

        public string Name { get; private set; }

        public string ResourceName { get; private set; }

        public int ReplicationTimeoutInMs { get; private set; }

        public unsafe CounterStorage(string serverUrl, string storageName, InMemoryRavenConfiguration configuration, TransportState receivedTransportState = null)
        {
            CounterStorageUrl = $"{serverUrl}cs/{storageName}";
            Name = storageName;
            ResourceName = string.Concat(Constants.Counter.UrlPrefix, "/", storageName);

            var options = configuration.RunInMemory ? StorageEnvironmentOptions.CreateMemoryOnly(configuration.Storage.Voron.TempPath)
                : CreateStorageOptionsFromConfiguration(configuration.Counter.DataDirectory, configuration.Settings);

            storageEnvironment = new StorageEnvironment(options);
            transportState = receivedTransportState ?? new TransportState();
            notificationPublisher = new NotificationPublisher(transportState);
            replicationTask = new ReplicationTask(this);
            ReplicationTimeoutInMs = configuration.Replication.ReplicationRequestTimeoutInMilliseconds;
            tombstoneRetentionTime = configuration.Counter.TombstoneRetentionTime;
            deletedTombstonesInBatch = configuration.Counter.DeletedTombstonesInBatch;
            metricsCounters = new CountersMetricsManager();
            Configuration = configuration;
            ExtensionsState = new AtomicDictionary<object>();
            jsonSerializer = new JsonSerializer();
            sizeOfGuid = sizeof(Guid);

            Initialize();
            purgeTombstonesTimer = new Timer(BackgroundActionsCallback, null, TimeSpan.Zero, TimeSpan.FromHours(1));
        }

        private void Initialize()
        {
            using (var tx = Environment.NewTransaction(TransactionFlags.ReadWrite))
            {
                storageEnvironment.CreateTree(tx, TreeNames.ServersLastEtag);
                storageEnvironment.CreateTree(tx, TreeNames.ReplicationSources);
                storageEnvironment.CreateTree(tx, TreeNames.Counters);
                storageEnvironment.CreateTree(tx, TreeNames.DateToTombstones);
                storageEnvironment.CreateTree(tx, TreeNames.GroupToCounters);
                storageEnvironment.CreateTree(tx, TreeNames.TombstonesGroupToCounters);
                storageEnvironment.CreateTree(tx, TreeNames.CounterIdWithNameToGroup);
                storageEnvironment.CreateTree(tx, TreeNames.CountersToEtag);

                var etags = Environment.CreateTree(tx, TreeNames.EtagsToCounters);
                var metadata = Environment.CreateTree(tx, TreeNames.Metadata);
                var id = metadata.Read("id");
                var lastCounterIdRead = metadata.Read("lastCounterId");

                if (id == null) // new counter db
                {
                    ServerId = Guid.NewGuid();
                    var serverIdBytes = ServerId.ToByteArray();
                    metadata.Add("id", serverIdBytes);
                }
                else // existing counter db
                {
                    int used;
                    ServerId = new Guid(id.Reader.ReadBytes(sizeOfGuid, out used));


                    using (var it = etags.Iterate(false))
                    {
                        if (it.Seek(Slice.AfterAllKeys))
                        {
                            lastEtag = it.CurrentKey.CreateReader().ReadBigEndianInt64();
                        }
                    }
                }

                if (lastCounterIdRead == null)
                {
                    var buffer = new byte[sizeof(long)];
                    var slice = new Slice(buffer);
                    metadata.Add("lastCounterId", slice);
                    lastCounterId = 0;
                }
                else
                {
                    lastCounterId = lastCounterIdRead.Reader.ReadBigEndianInt64();
                }

                tx.Commit();

                replicationTask.StartReplication();
            }
        }

        private void BackgroundActionsCallback(object state)
        {
            try
            {
                while (true)
                {
                    using (var writer = CreateWriter())
                    {
                        if (writer.PurgeOutdatedTombstones(tombstoneRetentionTime) == false)
                            break;

                        writer.Commit();
                    }
                }
            }
            catch (Exception e)
            {
                Log.Info("Failed to purge outdated tombstones. going to try again in an hour.", e.Message);
            }
        }

        string IResourceStore.Name => Name;

        [CLSCompliant(false)]
        public CountersMetricsManager MetricsCounters => metricsCounters;

        public TransportState TransportState => transportState;

        public NotificationPublisher Publisher => notificationPublisher;

        public ReplicationTask ReplicationTask => replicationTask;

        public StorageEnvironment Environment => storageEnvironment;

        private JsonSerializer JsonSerializer => jsonSerializer;

        public AtomicDictionary<object> ExtensionsState { get; private set; }

        public InMemoryRavenConfiguration Configuration { get; private set; }

        public CounterStorageStats CreateStats()
        {
            using (var reader = CreateReader())
            {
                var stats = new CounterStorageStats
                {
                    Name = Name,
                    Url = CounterStorageUrl,
                    CountersCount = reader.GetCountersCount(),
                    GroupsCount = reader.GetGroupsCount(),
                    TombstonesCount = reader.GetTombsotnesCount(),
                    LastCounterEtag = lastEtag,
                    ReplicationTasksCount = replicationTask.GetActiveTasksCount(),
                    CounterStorageSize = SizeHelper.Humane(Environment.Stats().UsedDataFileSizeInBytes),
                    ReplicatedServersCount = replicationTask.DestinationStats.Count,
                    RequestsPerSecond = Math.Round(metricsCounters.RequestsPerSecondCounter.CurrentValue, 3),
                };
                return stats;
            }
        }

        public CountersStorageMetrics CreateMetrics()
        {
            var metrics = metricsCounters;

            return new CountersStorageMetrics
            {
                RequestsPerSecond = Math.Round(metrics.RequestsPerSecondCounter.CurrentValue, 3),
                Resets = metrics.Resets.CreateMeterData(),
                Increments = metrics.Increments.CreateMeterData(),
                Decrements = metrics.Decrements.CreateMeterData(),
                ClientRequests = metrics.ClientRequests.CreateMeterData(),
                IncomingReplications = metrics.IncomingReplications.CreateMeterData(),
                OutgoingReplications = metrics.OutgoingReplications.CreateMeterData(),

                RequestsDuration = metrics.RequestDurationMetric.CreateHistogramData(),
                IncSizes = metrics.IncSizeMetrics.CreateHistogramData(),
                DecSizes = metrics.DecSizeMetrics.CreateHistogramData(),

                ReplicationBatchSizeMeter = metrics.ReplicationBatchSizeMeter.ToMeterDataDictionary(),
                ReplicationBatchSizeHistogram = metrics.ReplicationBatchSizeHistogram.ToHistogramDataDictionary(),
                ReplicationDurationHistogram = metrics.ReplicationDurationHistogram.ToHistogramDataDictionary()
            };
        }

        private static StorageEnvironmentOptions CreateStorageOptionsFromConfiguration(string path, NameValueCollection settings)
        {
            bool result;
            if (bool.TryParse(settings[Constants.RunInMemory] ?? "false", out result) && result)
                return StorageEnvironmentOptions.CreateMemoryOnly(settings[Constants.Voron.TempPath]);

            bool allowIncrementalBackupsSetting;
            if (bool.TryParse(settings[Constants.Voron.AllowIncrementalBackups] ?? "false", out allowIncrementalBackupsSetting) == false)
                throw new ArgumentException(Constants.Voron.AllowIncrementalBackups + " settings key contains invalid value");

            var directoryPath = path ?? AppDomain.CurrentDomain.BaseDirectory;
            var filePathFolder = new DirectoryInfo(directoryPath);
            if (filePathFolder.Exists == false)
                filePathFolder.Create();

            var tempPath = settings[Constants.Voron.TempPath];
            var journalPath = settings[Constants.RavenTxJournalPath];
            var options = StorageEnvironmentOptions.ForPath(directoryPath, tempPath, journalPath);
            options.IncrementalBackupEnabled = allowIncrementalBackupsSetting;
            return options;
        }

        [CLSCompliant(false)]
        public Reader CreateReader()
        {
            return new Reader(this, Environment.NewTransaction(TransactionFlags.Read));
        }

        [CLSCompliant(false)]
        public Writer CreateWriter()
        {
            return new Writer(this, Environment.NewTransaction(TransactionFlags.ReadWrite));
        }

        private void Notify()
        {
            CounterUpdated();
        }

        public void Dispose()
        {
            var exceptionAggregator = new ExceptionAggregator(Log, "Could not properly dispose of CounterStorage: " + Name);

            if (replicationTask != null)
                exceptionAggregator.Execute(replicationTask.Dispose);

            if (storageEnvironment != null)
                exceptionAggregator.Execute(storageEnvironment.Dispose);

            if (metricsCounters != null)
                exceptionAggregator.Execute(metricsCounters.Dispose);

            if (purgeTombstonesTimer != null)
                purgeTombstonesTimer.Dispose();

            exceptionAggregator.ThrowIfNeeded();
        }

        [CLSCompliant(false)]
        public class Reader : IDisposable
        {
            private readonly Transaction transaction;
            private readonly Tree counters,
                dateToTombstones, 
                groupToCounters, 
                tombstonesGroupToCounters, 
                counterIdWithNameToGroup, 
                etagsToCounters, 
                countersToEtag, 
                serversLastEtag, 
                replicationSources, 
                metadata;
            private readonly CounterStorage parent;

            [CLSCompliant(false)]
            public Reader(CounterStorage parent, Transaction transaction)
            {
                this.transaction = transaction;
                this.parent = parent;
                counters = transaction.ReadTree(TreeNames.Counters);
                dateToTombstones = transaction.ReadTree(TreeNames.DateToTombstones);
                groupToCounters = transaction.ReadTree(TreeNames.GroupToCounters);
                tombstonesGroupToCounters = transaction.ReadTree(TreeNames.TombstonesGroupToCounters);
                counterIdWithNameToGroup = transaction.ReadTree(TreeNames.CounterIdWithNameToGroup);
                countersToEtag = transaction.ReadTree(TreeNames.CountersToEtag);
                etagsToCounters = transaction.ReadTree(TreeNames.EtagsToCounters);
                serversLastEtag = transaction.ReadTree(TreeNames.ServersLastEtag);
                replicationSources = transaction.ReadTree(TreeNames.ReplicationSources);
                metadata = transaction.ReadTree(TreeNames.Metadata);
            }

            public long GetCountersCount()
            {
                ThrowIfDisposed();
                long countersCount = 0;
                using (var it = groupToCounters.Iterate())
                {
                    if (it.Seek(Slice.BeforeAllKeys) == false)
                        return countersCount;

                    do
                    {
                        countersCount += groupToCounters.MultiCount(it.CurrentKey);
                    } while (it.MoveNext());
                }
                return countersCount;
            }

            public long GetTombsotnesCount()
            {
                ThrowIfDisposed();
                long tombsotnesCount = 0;
                using (var it = tombstonesGroupToCounters.Iterate())
                {
                    if (it.Seek(Slice.BeforeAllKeys) == false)
                        return tombsotnesCount;

                    do
                    {
                        tombsotnesCount += tombstonesGroupToCounters.MultiCount(it.CurrentKey);
                    } while (it.MoveNext());
                }
                return tombsotnesCount;
            }

            public long GetGroupsCount()
            {
                ThrowIfDisposed();
                return groupToCounters.State.EntriesCount;
            }

            internal IEnumerable<CounterDetails> GetCountersDetails(string groupName, int skip)
            {
                ThrowIfDisposed();
                using (var it = groupToCounters.Iterate())
                {
                    it.RequiredPrefix = groupName;
                    if (it.Seek(it.RequiredPrefix) == false)
                        yield break;

                    do
                    {
                        var countersInGroup = groupToCounters.MultiCount(it.CurrentKey);
                        if (skip - countersInGroup <= 0)
                            break;

                        //there is no better way than this, since we do not know
                        //how many counters in each group beforehand
                        skip -= (int)countersInGroup; 
                    } while (it.MoveNext());

                    var isEmptyGroup = groupName.Equals(string.Empty);
                    do
                    {
                        using (var iterator = groupToCounters.MultiRead(it.CurrentKey))
                        {
                            if (iterator.Seek(Slice.BeforeAllKeys) == false || (skip > 0 && !iterator.Skip(skip)))
                            {
                                skip = 0;
                                continue;
                            }
                            skip = 0;

                            do
                            {
                                var counterDetails = new CounterDetails
                                {
                                    Group = isEmptyGroup ? it.CurrentKey.ToString() : groupName
                                };
                                var valueReader = iterator.CurrentKey.CreateReader();
                                var requiredNameBufferSize = iterator.CurrentKey.Size - sizeof(long);

                                int used;
                                var counterNameBuffer = valueReader.ReadBytes(requiredNameBufferSize, out used);
                                counterDetails.Name = Encoding.UTF8.GetString(counterNameBuffer, 0, requiredNameBufferSize);

                                var counterIdBuffer = valueReader.ReadBytes(sizeof(long), out used);
                                counterDetails.IdSlice = new Slice(counterIdBuffer, sizeof(long));

                                yield return counterDetails;
                            } while (iterator.MoveNext());
                        }
                    } while (isEmptyGroup && it.MoveNext());
                }
            }

            public IEnumerable<CounterSummary> GetCounterSummariesByGroup(string groupName, int skip, int take)
            {
                ThrowIfDisposed();
                var countersDetails = (take != -1) ? GetCountersDetails(groupName, skip).Take(take) : GetCountersDetails(groupName, skip);
                
                var serverIdBuffer = new byte[parent.sizeOfGuid];
                return countersDetails.Select(counterDetails => new CounterSummary
                {
                    GroupName = counterDetails.Group,
                    CounterName = counterDetails.Name,
                    Increments = CalculateCounterTotalChangeBySign(counterDetails.IdSlice, serverIdBuffer, ValueSign.Positive),
                    Decrements = CalculateCounterTotalChangeBySign(counterDetails.IdSlice, serverIdBuffer, ValueSign.Negative)
                });
            }

            private long CalculateCounterTotalChangeBySign(Slice counterIdSlice, byte[] serverIdBuffer, char signToCalculate)
            {
                using (var it = counters.Iterate())
                {
                    it.RequiredPrefix = counterIdSlice;
                    var seekResult = it.Seek(it.RequiredPrefix);
                    //should always be true
                    Debug.Assert(seekResult);

                    long totalChangeBySign = 0;
                    do
                    {
                        var reader = it.CurrentKey.CreateReader();
                        reader.Skip(sizeof(long));
                        reader.Read(serverIdBuffer, 0, parent.sizeOfGuid);
                        var serverId = new Guid(serverIdBuffer);
                        //this means that this is a tombstone of a counter
                        if (serverId.Equals(parent.tombstoneId))
                            continue;

                        var lastByte = it.CurrentKey[it.CurrentKey.Size - 1];
                        var sign = Convert.ToChar(lastByte);
                        Debug.Assert(sign == ValueSign.Positive || sign == ValueSign.Negative);
                        var value = it.CreateReaderForCurrent().ReadLittleEndianInt64();
                        if (sign == signToCalculate)
                            totalChangeBySign += value;
                    } while (it.MoveNext());

                    return totalChangeBySign;
                }
            }

            private long CalculateCounterTotal(Slice counterIdSlice, byte[] serverIdBuffer)
            {
                using (var it = counters.Iterate())
                {
                    it.RequiredPrefix = counterIdSlice;
                    var seekResult = it.Seek(it.RequiredPrefix);
                    //should always be true
                    Debug.Assert(seekResult == true);

                    long total = 0;
                    do
                    {
                        var reader = it.CurrentKey.CreateReader();
                        reader.Skip(sizeof(long));
                        reader.Read(serverIdBuffer, 0, parent.sizeOfGuid);
                        var serverId = new Guid(serverIdBuffer);
                        //this means that this is a tombstone of a counter
                        if (serverId.Equals(parent.tombstoneId))
                            continue;

                        var lastByte = it.CurrentKey[it.CurrentKey.Size - 1];
                        var sign = Convert.ToChar(lastByte);
                        Debug.Assert(sign == ValueSign.Positive || sign == ValueSign.Negative);
                        var value = it.CreateReaderForCurrent().ReadLittleEndianInt64();
                        if (sign == ValueSign.Positive)
                            total += value;
                        else
                            total -= value;
                    } while (it.MoveNext());

                    return total;
                }
            }


            public bool TryGetCounterTotal(string groupName, string counterName,out long? total)
            {
                ThrowIfDisposed();
                total = null;
                using (var it = groupToCounters.MultiRead(groupName))
                {
                    it.RequiredPrefix = counterName;
                    if (it.Seek(it.RequiredPrefix) == false || it.CurrentKey.Size != it.RequiredPrefix.Size + sizeof (long))
                        return false;

                    var valueReader = it.CurrentKey.CreateReader();
                    valueReader.Skip(it.RequiredPrefix.Size);
                    int used;
                    var counterIdBuffer = valueReader.ReadBytes(sizeof(long), out used);
                    var slice = new Slice(counterIdBuffer, sizeof(long));
                    total = CalculateCounterTotal(slice, new byte[parent.sizeOfGuid]);
                    return true;
                }
            }

            public long GetGroupCount()
            {
                ThrowIfDisposed();
                long count = 0;
                using (var it = groupToCounters.Iterate())
                {
                    if (it.Seek(Slice.BeforeAllKeys) == false)
                        return 0;

                    do
                    {
                        count ++;
                    } while (it.MoveNext());

                }
                return count;
            }

            public IEnumerable<CounterSummary> GetCounterSummariesByPrefix(string groupName, string counterNamePrefix, int skip, int take)
            {
                ThrowIfDisposed();
                using (var it = groupToCounters.MultiRead(groupName))
                {
                    if (!it.Seek(Slice.BeforeAllKeys))
                        yield break;
                    if (!string.IsNullOrEmpty(counterNamePrefix))
                    {
                        it.RequiredPrefix = counterNamePrefix;
                        it.Seek(it.RequiredPrefix);
                    }

                    var taken = 0;
                    var skipped = 0;
                    do
                    {
                        if(skipped++ < skip)
                            continue;

                        var reader = it.CurrentKey.CreateReader();
                        var counterNameLength = reader.Length - sizeof(long);
                        
                        var counterName = reader.ReadAsString(counterNameLength);
                        var counterIdSlice = reader.ReadAsSlice(sizeof(long));
                        var counter = GetCounterByCounterId(counterIdSlice);

                        Debug.Assert(counter.ServerValues != null,"counter.Ser//verValues != null");

                        yield return new CounterSummary
                        {
                            CounterName = counterName,
                            GroupName = groupName,
                            Increments = counter.ServerValues.Where(x => x.Value >= 0).Sum(x => x.Value),
                            Decrements = counter.ServerValues.Where(x => x.Value < 0).Sum(x => x.Value)                            
                        };
                    } while (it.MoveNext() && taken++ < take);
                }
            }

            public IEnumerable<CounterGroup> GetCounterGroups(int skip, int take)
            {
                ThrowIfDisposed();
                using (var it = groupToCounters.Iterate())
                {
                    if (it.Seek(Slice.BeforeAllKeys) == false)
                        yield break;

                    if (skip > 0 && !it.Skip(skip))
                        yield break;

                    var taken = 0;
                    do
                    {						
                        yield return new CounterGroup
                        {
                            Name = it.CurrentKey.ToString(),
                            Count = groupToCounters.MultiCount(it.CurrentKey)
                        };
                    } while (it.MoveNext() && taken++ < take);
                }
            }

            //{counterId}{serverId}{sign}
            internal long GetSingleCounterValue(Slice singleCounterName)
            {
                ThrowIfDisposed();
                var readResult = counters.Read(singleCounterName);
                if (readResult == null)
                    return -1;

                return readResult.Reader.ReadLittleEndianInt64();
            }

            private Counter GetCounterByCounterId(Slice counterIdSlice)
            {
                var counter = new Counter { LocalServerId = parent.ServerId };
                using (var it = counters.Iterate())
                {
                    it.RequiredPrefix = counterIdSlice;
                    var seekResult = it.Seek(it.RequiredPrefix);
                                        
                    // ReSharper disable once RedundantBoolCompare
                    Debug.Assert(seekResult == true);

                    var serverIdBuffer = new byte[parent.sizeOfGuid];
                    long lastEtag = 0;
                    do
                    {
                        var reader = it.CurrentKey.CreateReader();
                        reader.Skip(sizeof(long));
                        reader.Read(serverIdBuffer, 0, parent.sizeOfGuid);
                        var serverId = new Guid(serverIdBuffer);
                        //this means that this is a tombstone of a counter
                        if (serverId.Equals(parent.tombstoneId))
                            continue;

                        var serverValue = new ServerValue { ServerId = serverId, ServerName = GetSourceNameFor(serverIdBuffer) };
                        var etagResult = countersToEtag.Read(it.CurrentKey);
                        Debug.Assert(etagResult != null);
                        serverValue.Etag = etagResult.Reader.ReadBigEndianInt64();
                        if (lastEtag < serverValue.Etag)
                        {
                            counter.LastUpdateByServer = serverId;
                            lastEtag = serverValue.Etag;
                        }

                        var lastByte = it.CurrentKey[it.CurrentKey.Size - 1];
                        var sign = Convert.ToChar(lastByte);
                        Debug.Assert(sign == ValueSign.Positive || sign == ValueSign.Negative);
                        var value = it.CreateReaderForCurrent().ReadLittleEndianInt64();
                        if (sign == ValueSign.Negative)
                            value = -value;
                        serverValue.Value += value;
                        counter.ServerValues.Add(serverValue);
                    } while (it.MoveNext());
                }

                return counter;
            }

            public Counter GetCounter(string groupName, string counterName)
            {
                ThrowIfDisposed();
                using (var it = groupToCounters.MultiRead(groupName))
                {
                    it.RequiredPrefix = counterName;
                    if (it.Seek(it.RequiredPrefix) == false || it.CurrentKey.Size != it.RequiredPrefix.Size + sizeof(long))
                        throw new Exception("Counter doesn't exist!");

                    var valueReader = it.CurrentKey.CreateReader(); 
                    valueReader.Skip(it.RequiredPrefix.Size);
                    int used;
                    var counterIdBuffer = valueReader.ReadBytes(sizeof(long), out used);
                    var counterIdSlice = new Slice(counterIdBuffer, sizeof(long));

                    return GetCounterByCounterId(counterIdSlice);
                }
            }

            public IEnumerable<ReplicationSourceInfo> GetReplicationSources()
            {
                ThrowIfDisposed();
                var lookupDict = GetServerEtags().ToDictionary(x => x.ServerId, x => x.Etag);

                using (var it = replicationSources.Iterate())
                {
                    if (it.Seek(Slice.BeforeAllKeys) == false)
                        yield break;

                    var serverIdBuffer = new byte[parent.sizeOfGuid];
                    do
                    {
                        it.CurrentKey.CreateReader().Read(serverIdBuffer, 0, serverIdBuffer.Length);
                        var serverId = new Guid(serverIdBuffer);

                        yield return new ReplicationSourceInfo
                        {
                            ServerId = serverId,
                            SourceName = it.CreateReaderForCurrent().ToString(),
                            Etag = lookupDict[serverId]
                        };

                    } while (it.MoveNext());
                }
            }

            public IEnumerable<ServerEtag> GetServerEtags()
            {
                ThrowIfDisposed();
                using (var it = serversLastEtag.Iterate())
                {
                    if (it.Seek(Slice.BeforeAllKeys) == false)
                        yield break;

                    var serverIdBuffer = new byte[parent.sizeOfGuid];
                    do
                    {
                        it.CurrentKey.CreateReader().Read(serverIdBuffer, 0, serverIdBuffer.Length);

                        yield return new ServerEtag
                        {
                            ServerId = new Guid(serverIdBuffer),
                            Etag = it.CreateReaderForCurrent().ReadBigEndianInt64()
                        };
                    } while (it.MoveNext());
                }
            }

            public IEnumerable<CounterState> GetCountersSinceEtag(long etag, int skip = 0, int take = int.MaxValue)
            {
                ThrowIfDisposed();
                using (var it = etagsToCounters.Iterate())
                {
                    var buffer = new byte[sizeof(long)];
                    EndianBitConverter.Big.CopyBytes(etag, buffer, 0);
                    var slice = new Slice(buffer);
                    if (it.Seek(slice) == false || it.Skip(skip) == false)
                        yield break;

                    int taken = 0;
                    var serverIdBuffer = new byte[parent.sizeOfGuid];
                    var signBuffer = new byte[sizeof(char)];
                    do
                    {
                        //{counterId}{serverId}{sign}
                        var valueReader = it.CreateReaderForCurrent();
                        int used;
                        var counterIdBuffer = valueReader.ReadBytes(sizeof(long), out used);
                        var counterIdSlice = new Slice(counterIdBuffer, sizeof(long));
                        valueReader.Read(serverIdBuffer, 0, parent.sizeOfGuid);
                        var serverId = new Guid(serverIdBuffer);

                        valueReader.Read(signBuffer, 0, sizeof(char));
                        var sign = EndianBitConverter.Big.ToChar(signBuffer, 0);
                        Debug.Assert(sign == ValueSign.Positive || sign == ValueSign.Negative);

                        //single counter names: {counter-id}{server-id}{sign}
                        var singleCounterName = valueReader.AsSlice();
                        var value = GetSingleCounterValue(singleCounterName);

                        //read counter name and group
                        var counterNameAndGroup = GetCounterNameAndGroupByServerId(counterIdSlice);
                        var etagResult = countersToEtag.Read(singleCounterName);
                        Debug.Assert(etagResult != null);
                        var counterEtag = etagResult.Reader.ReadBigEndianInt64();

                        yield return new CounterState
                        {
                            GroupName = counterNameAndGroup.GroupName,
                            CounterName = counterNameAndGroup.CounterName,
                            ServerId = serverId,
                            Sign = sign,
                            Value = value,
                            Etag = counterEtag
                        };
                        taken++;
                    } while (it.MoveNext() && taken < take);
                }
            }

            private class CounterNameAndGroup
            {
                public string CounterName { get; set; }
                public string GroupName { get; set; }
            }

            private CounterNameAndGroup GetCounterNameAndGroupByServerId(Slice counterIdSlice)
            {
                var counterNameAndGroup = new CounterNameAndGroup();
                using (var it = counterIdWithNameToGroup.Iterate(false))
                {
                    it.RequiredPrefix = counterIdSlice;
                    if (it.Seek(it.RequiredPrefix) == false)
                        throw new InvalidOperationException("Couldn't find counter id!");

                    var counterNameSize = it.CurrentKey.Size - sizeof(long);
                    var reader = it.CurrentKey.CreateReader();
                    reader.Skip(sizeof(long));
                    int used;
                    var counterNameBuffer = reader.ReadBytes(counterNameSize, out used);
                    counterNameAndGroup.CounterName = Encoding.UTF8.GetString(counterNameBuffer, 0, counterNameSize);

                    var valueReader = it.CreateReaderForCurrent();
                    var groupNameBuffer = valueReader.ReadBytes(valueReader.Length, out used);
                    counterNameAndGroup.GroupName = Encoding.UTF8.GetString(groupNameBuffer, 0, valueReader.Length);
                }
                return counterNameAndGroup;
            }

            public long GetLastEtagFor(Guid serverId)
            {
                ThrowIfDisposed();
                var slice = new Slice(serverId.ToByteArray());
                var readResult = serversLastEtag.Read(slice);
                return readResult != null ? readResult.Reader.ReadBigEndianInt64() : 0;
            }

            private string GetSourceNameFor(byte[] serverIdBuffer)
            {
                ThrowIfDisposed();
                var slice = new Slice(serverIdBuffer);
                var readResult = replicationSources.Read(slice);
                //if we can't find the server id in the replication sources, this is the local server
                if (readResult == null)
                    return null;

                var reader = readResult.Reader;
                int used;
                var sourceNameBuffer = reader.ReadBytes(reader.Length, out used);
                return Encoding.UTF8.GetString(sourceNameBuffer, 0, reader.Length);
            }

            public CountersReplicationDocument GetReplicationData()
            {
                ThrowIfDisposed();
                var readResult = metadata.Read("replication");
                if (readResult == null)
                    return null;

                var stream = readResult.Reader.AsStream();
                stream.Position = 0;
                using (var streamReader = new StreamReader(stream))
                using (var jsonTextReader = new JsonTextReader(streamReader))
                {
                    return new JsonSerializer().Deserialize<CountersReplicationDocument>(jsonTextReader);
                }
            }

            public BackupStatus GetBackupStatus()
            {
                ThrowIfDisposed();
                var readResult = metadata.Read(BackupStatus.RavenBackupStatusDocumentKey);
                if (readResult == null)
                    return null;

                var stream = readResult.Reader.AsStream();
                stream.Position = 0;
                using (var streamReader = new StreamReader(stream))
                using (var jsonTextReader = new JsonTextReader(streamReader))
                {
                    return new JsonSerializer().Deserialize<BackupStatus>(jsonTextReader);
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            [Conditional("DEBUG")]
            private void ThrowIfDisposed()
            {
                if (transaction.IsDisposed)
                    throw new ObjectDisposedException("CounterStorage::Reader", "The reader should not be used after being disposed.");
            }

            public void Dispose()
            {
                if (transaction != null)
                    transaction.Dispose();
            }
        }

        [CLSCompliant(false)]
        public class Writer : IDisposable
        {
            private readonly CounterStorage parent;
            private readonly Transaction transaction;
            private readonly Reader reader;
            private readonly Tree counters,
                dateToTombstones,
                groupToCounters,
                tombstonesGroupToCounters,
                counterIdWithNameToGroup,
                etagsToCounters,
                countersToEtag,
                serversLastEtag,
                replicationSources,
                metadata;
            private readonly Buffer buffer;

            private class Buffer
            {
                public Buffer(int sizeOfGuid)
                {
                    FullCounterName = new byte[sizeof(long) + sizeOfGuid + sizeof(char)];
                    ServerId = new byte[sizeOfGuid];
                }

                public readonly byte[] FullCounterName;
                public readonly byte[] ServerId;
                public readonly byte[] CounterValue = new byte[sizeof(long)];
                public readonly byte[] Etag = new byte[sizeof(long)];
                public readonly byte[] CounterId = new byte[sizeof(long)];
                public readonly Lazy<byte[]> TombstoneTicks = new Lazy<byte[]>(() => new byte[sizeof(long)]);

                public byte[] GroupName = new byte[0];
                public byte[] CounterNameWithId = new byte[0];
            }

            public string Name => parent.Name;

            public Writer(CounterStorage parent, Transaction tx)
            {
                if (tx.Flags != TransactionFlags.ReadWrite) //precaution
                    throw new InvalidOperationException(string.Format("Counters writer cannot be created with read-only transaction. (tx id = {0})", transaction.Id));

                this.parent = parent;
                transaction = tx;
                reader = new Reader(parent, transaction);
                counters = transaction.ReadTree(TreeNames.Counters);
                dateToTombstones = transaction.ReadTree(TreeNames.DateToTombstones);
                groupToCounters = transaction.ReadTree(TreeNames.GroupToCounters);
                tombstonesGroupToCounters = transaction.ReadTree(TreeNames.TombstonesGroupToCounters);
                counterIdWithNameToGroup = transaction.ReadTree(TreeNames.CounterIdWithNameToGroup);
                countersToEtag = transaction.ReadTree(TreeNames.CountersToEtag);
                etagsToCounters = transaction.ReadTree(TreeNames.EtagsToCounters);
                serversLastEtag = transaction.ReadTree(TreeNames.ServersLastEtag);
                replicationSources = transaction.ReadTree(TreeNames.ReplicationSources);

                metadata = transaction.ReadTree(TreeNames.Metadata);
                buffer = new Buffer(parent.sizeOfGuid);
            }

            public long GetLastEtagFor(Guid serverId)
            {
                ThrowIfDisposed();
                return reader.GetLastEtagFor(serverId);
            }

            public bool TryGetCounterTotal(string groupName, string counterName, out long? total)
            {
                ThrowIfDisposed();
                return reader.TryGetCounterTotal(groupName, counterName, out total);
            }

            internal IEnumerable<CounterDetails> GetCountersDetails(string groupName)
            {
                ThrowIfDisposed();
                return reader.GetCountersDetails(groupName, 0);
            }

            //local counters
            public CounterChangeAction Store(string groupName, string counterName, long delta)
            {
                ThrowIfDisposed();
                var sign = delta >= 0 ? ValueSign.Positive : ValueSign.Negative;
                var doesCounterExist = Store(groupName, counterName, parent.ServerId, sign, counterKeySlice =>
                {
                    if (sign == ValueSign.Negative)
                        delta = Math.Abs(delta);
                    counters.Increment(counterKeySlice, delta);
                });

                if (doesCounterExist)
                    return sign == ValueSign.Positive ? CounterChangeAction.Increment : CounterChangeAction.Decrement;

                return CounterChangeAction.Add;
            }

            //counters from replication
            public CounterChangeAction Store(string groupName, string counterName, Guid serverId, char sign, long value)
            {
                ThrowIfDisposed();
                var doesCounterExist = Store(groupName, counterName, serverId, sign, counterKeySlice =>
                {
                    //counter value is little endian
                    EndianBitConverter.Little.CopyBytes(value, buffer.CounterValue, 0);
                    var counterValueSlice = new Slice(buffer.CounterValue);
                    counters.Add(counterKeySlice, counterValueSlice);

                    if (serverId.Equals(parent.tombstoneId))
                    {
                        //tombstone key is big endian
                        Array.Reverse(buffer.CounterValue);
                        var tombstoneKeySlice = new Slice(buffer.CounterValue);
                        dateToTombstones.MultiAdd(tombstoneKeySlice, counterKeySlice);
                    }
                });

                if (serverId.Equals(parent.tombstoneId))
                    return CounterChangeAction.Delete;

                if (doesCounterExist)
                    return value >= 0 ? CounterChangeAction.Increment : CounterChangeAction.Decrement;

                return CounterChangeAction.Add;
            }

            // full counter name: foo/bar/server-id/+
            private bool Store(string groupName, string counterName, Guid serverId, char sign, Action<Slice> storeAction)
            {
                var groupNameSlice = SliceFrom(groupName);
                var counterIdBuffer = GetCounterIdBytes(groupToCounters, groupNameSlice, counterName);
                var doesCounterExist = counterIdBuffer != null;
                if (doesCounterExist == false)
                {
                    counterIdBuffer = GetCounterIdBytes(tombstonesGroupToCounters, groupNameSlice, counterName);
                    if (counterIdBuffer == null)
                    {
                        parent.lastCounterId++;
                        EndianBitConverter.Big.CopyBytes(parent.lastCounterId, buffer.CounterId, 0);
                        var slice = new Slice(buffer.CounterId);
                        metadata.Add("lastCounterId", slice);
                        counterIdBuffer = buffer.CounterId;
                    }
                }

                UpdateGroups(counterName, counterIdBuffer, serverId, groupNameSlice);

                var counterKeySlice = GetFullCounterNameSlice(counterIdBuffer, serverId, sign);
                storeAction(counterKeySlice);

                RemoveOldEtagIfNeeded(counterKeySlice);
                UpdateCounterMetadata(counterKeySlice);

                return doesCounterExist;
            }

            private void UpdateGroups(string counterName, byte[] counterId, Guid serverId, Slice groupNameSlice)
            {
                var counterNameWithIdSize = Encoding.UTF8.GetByteCount(counterName) + sizeof(long);
                Debug.Assert(counterNameWithIdSize < ushort.MaxValue);
                EnsureProperBufferSize(ref buffer.CounterNameWithId, counterNameWithIdSize);
                var sliceWriter = new SliceWriter(buffer.CounterNameWithId);
                sliceWriter.Write(counterName);
                sliceWriter.Write(counterId);
                var counterNameWithIdSlice = sliceWriter.CreateSlice(counterNameWithIdSize);

                if (serverId.Equals(parent.tombstoneId))
                {
                    //if it's a tombstone, we can remove the counter from the groupToCounters Tree
                    //and add it to the tombstonesGroupToCounters tree
                    groupToCounters.MultiDelete(groupNameSlice, counterNameWithIdSlice);
                    tombstonesGroupToCounters.MultiAdd(groupNameSlice, counterNameWithIdSlice);
                }
                else
                {
                    //if it's not a tombstone, we need to add it to the groupToCounters Tree
                    //and remove it from the tombstonesGroupToCounters tree
                    groupToCounters.MultiAdd(groupNameSlice, counterNameWithIdSlice);
                    tombstonesGroupToCounters.MultiDelete(groupNameSlice, counterNameWithIdSlice);

                    DeleteExistingTombstone(counterId);
                }

                sliceWriter.Reset();
                sliceWriter.Write(counterId);
                sliceWriter.Write(counterName);
                var idWithCounterNameSlice = sliceWriter.CreateSlice(counterNameWithIdSize);

                counterIdWithNameToGroup.Add(idWithCounterNameSlice, groupNameSlice);
            }

            private Slice GetFullCounterNameSlice(byte[] counterIdBytes, Guid serverId, char sign)
            {
                var sliceWriter = new SliceWriter(buffer.FullCounterName);
                sliceWriter.Write(counterIdBytes);
                sliceWriter.Write(serverId.ToByteArray());
                sliceWriter.Write(sign);
                return sliceWriter.CreateSlice();
            }

            private byte[] GetCounterIdBytes(Tree tree, Slice groupNameSlice, string counterName)
            {
                using (var it = tree.MultiRead(groupNameSlice))
                {
                    it.RequiredPrefix = counterName;
                    if (it.Seek(it.RequiredPrefix) == false || 
                        it.CurrentKey.Size != it.RequiredPrefix.Size + sizeof(long))
                        return null;

                    var valueReader = it.CurrentKey.CreateReader();
                    valueReader.Skip(it.RequiredPrefix.Size);
                    valueReader.Read(buffer.CounterId, 0, sizeof(long));
                    return buffer.CounterId;
                }
            }

            private void DeleteExistingTombstone(byte[] counterIdBuffer)
            {
                var tombstoneNameSlice = GetFullCounterNameSlice(counterIdBuffer, parent.tombstoneId, ValueSign.Positive);
                var tombstone = counters.Read(tombstoneNameSlice);
                if (tombstone != null)
                {
                    //delete the tombstone from the tombstones tree
                    tombstone.Reader.Read(buffer.TombstoneTicks.Value, 0, sizeof (long));
                    Array.Reverse(buffer.TombstoneTicks.Value);
                    var slice = new Slice(buffer.TombstoneTicks.Value);
                    dateToTombstones.MultiDelete(slice, tombstoneNameSlice);
                }

                //Update the tombstone in the counters tree
                counters.Delete(tombstoneNameSlice);
                RemoveOldEtagIfNeeded(tombstoneNameSlice);
                countersToEtag.Delete(tombstoneNameSlice);
            }

            private void RemoveOldEtagIfNeeded(Slice counterKey)
            {
                var readResult = countersToEtag.Read(counterKey);
                if (readResult != null) // remove old etag entry
                {
                    readResult.Reader.Read(buffer.Etag, 0, sizeof(long));
                    var oldEtagSlice = new Slice(buffer.Etag);
                    etagsToCounters.Delete(oldEtagSlice);
                }
            }

            private void UpdateCounterMetadata(Slice counterKey)
            {
                parent.lastEtag++;
                EndianBitConverter.Big.CopyBytes(parent.lastEtag, buffer.Etag, 0);
                var newEtagSlice = new Slice(buffer.Etag);
                etagsToCounters.Add(newEtagSlice, counterKey);
                countersToEtag.Add(counterKey, newEtagSlice);
            }

            private bool DoesCounterExist(string groupName, string counterName)
            {
                using (var it = groupToCounters.MultiRead(groupName))
                {
                    it.RequiredPrefix = counterName;
                    if (it.Seek(it.RequiredPrefix) == false || it.CurrentKey.Size != it.RequiredPrefix.Size + sizeof(long))
                        return false;
                }

                return true;
            }

            public long Reset(string groupName, string counterName)
            {
                ThrowIfDisposed();
                var doesCounterExist = DoesCounterExist(groupName, counterName);
                if (doesCounterExist == false)
                    throw new InvalidOperationException($"Counter doesn't exist. Group: {groupName}, Counter Name: {counterName}");

                return ResetCounterInternal(groupName, counterName);
            }

            private long ResetCounterInternal(string groupName, string counterName)
            {
                long? total;
                if (!TryGetCounterTotal(groupName, counterName, out total))
                    return 0;

                Debug.Assert(total.HasValue);
                var difference = total.Value;
                if (difference == 0)
                    return 0;

                difference = -difference;
                Store(groupName, counterName, difference);
                return difference;
            }

            public void Delete(string groupName, string counterName)
            {
                ThrowIfDisposed();
                var counterExists = DoesCounterExist(groupName, counterName);
                if (counterExists == false)
                    throw new InvalidOperationException(string.Format("Counter doesn't exist. Group: {0}, Counter Name: {1}", groupName, counterName));

                DeleteCounterInternal(groupName, counterName);
            }

            internal void DeleteCounterInternal(string groupName, string counterName)
            {
                ThrowIfDisposed();
                ResetCounterInternal(groupName, counterName);
                Store(groupName, counterName, parent.tombstoneId, ValueSign.Positive, counterKeySlice =>
                {
                    //counter value is little endian
                    EndianBitConverter.Little.CopyBytes(DateTime.Now.Ticks, buffer.CounterValue, 0);
                    var counterValueSlice = new Slice(buffer.CounterValue);
                    counters.Add(counterKeySlice, counterValueSlice);

                    //all keys are big endian
                    Array.Reverse(buffer.CounterValue);
                    var tombstoneKeySlice = new Slice(buffer.CounterValue);
                    dateToTombstones.MultiAdd(tombstoneKeySlice, counterKeySlice);
                });
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool IsTombstone(Guid serverId)
            {
                return serverId.Equals(parent.tombstoneId);
            }

            public void RecordSourceNameFor(Guid serverId, string sourceName)
            {
                ThrowIfDisposed();
                var serverIdSlice = new Slice(serverId.ToByteArray());
                replicationSources.Add(serverIdSlice, new Slice(sourceName));
            }

            public void RecordLastEtagFor(Guid serverId, long lastEtag)
            {
                ThrowIfDisposed();
                var serverIdSlice = new Slice(serverId.ToByteArray());
                EndianBitConverter.Big.CopyBytes(lastEtag, buffer.Etag, 0);
                var etagSlice = new Slice(buffer.Etag);
                serversLastEtag.Add(serverIdSlice, etagSlice);
            }

            public class SingleCounterValue
            {
                public SingleCounterValue()
                {
                    Value = -1;
                }

                public long Value { get; set; }
                public bool DoesCounterExist { get; set; }
            }

            public SingleCounterValue GetSingleCounterValue(string groupName, string counterName, Guid serverId, char sign)
            {
                ThrowIfDisposed();
                var groupNameSlice = SliceFrom(groupName);
                var counterIdBuffer = GetCounterIdBytes(groupToCounters, groupNameSlice, counterName);
                var singleCounterValue = new SingleCounterValue { DoesCounterExist = counterIdBuffer != null };

                if (counterIdBuffer == null)
                {
                    //looking for the single counter in the tombstones tree
                    counterIdBuffer = GetCounterIdBytes(tombstonesGroupToCounters, groupNameSlice, counterName);
                    if (counterIdBuffer == null)
                        return singleCounterValue;
                }

                var fullCounterNameSlice = GetFullCounterNameSlice(counterIdBuffer, serverId, sign);
                singleCounterValue.Value = reader.GetSingleCounterValue(fullCounterNameSlice);
                return singleCounterValue;
            }

            private Slice SliceFrom(string str)
            {
                var groupSize = Encoding.UTF8.GetByteCount(str);
                Debug.Assert(groupSize < ushort.MaxValue);
                EnsureProperBufferSize(ref buffer.GroupName, groupSize);
                var sliceWriter = new SliceWriter(buffer.GroupName);
                sliceWriter.Write(str);
                var groupNameSlice = sliceWriter.CreateSlice(groupSize);
                return groupNameSlice;
            }

            public void UpdateReplications(CountersReplicationDocument newReplicationDocument)
            {
                ThrowIfDisposed();
                using (var memoryStream = new MemoryStream())
                using (var streamWriter = new StreamWriter(memoryStream))
                using (var jsonTextWriter = new JsonTextWriter(streamWriter))
                {
                    parent.JsonSerializer.Serialize(jsonTextWriter, newReplicationDocument);
                    streamWriter.Flush();
                    memoryStream.Position = 0;
                    metadata.Add("replication", memoryStream);
                }

                parent.replicationTask.SignalCounterUpdate();
            }

            public void SaveBackupStatus(BackupStatus backupStatus)
            {
                ThrowIfDisposed();
                using (var memoryStream = new MemoryStream())
                using (var streamWriter = new StreamWriter(memoryStream))
                using (var jsonTextWriter = new JsonTextWriter(streamWriter))
                {
                    parent.JsonSerializer.Serialize(jsonTextWriter, backupStatus);
                    streamWriter.Flush();
                    memoryStream.Position = 0;
                    metadata.Add(BackupStatus.RavenBackupStatusDocumentKey, memoryStream);
                }
            }

            public void DeleteBackupStatus()
            {
                ThrowIfDisposed();
                metadata.Delete(BackupStatus.RavenBackupStatusDocumentKey);
            }

            public bool PurgeOutdatedTombstones(TimeSpan? tombstoneRetentionTime = null)
            {
                ThrowIfDisposed();
                //if we're not provided with tombstoneRetentionTime, delete all the tombstones from the beginning of time
                
                //var timeAgo = DateTime.Now.AddTicks(-tombstoneRetentionTime.Value.Ticks);
                
                var deletedTombstonesInBatch = parent.deletedTombstonesInBatch;

                var slice = tombstoneRetentionTime != null ? GetTombstoneSlice(tombstoneRetentionTime.Value) : Slice.AfterAllKeys;
                var keysToDelete = new List<Tuple<Slice, Slice>>();
                using (var it = dateToTombstones.Iterate())
                {
                    if (it.Seek(slice) == false)
                        return false;

                    do
                    {
                        //look for coun
                        using (var innerIterator = dateToTombstones.MultiRead(it.CurrentKey))
                        {
                            innerIterator.Seek(Slice.BeforeAllKeys);
                            do
                            {
                                //{counter-id}{server-id}{sign}
                                //we need the counter id
                                var valueReader = innerIterator.CurrentKey.CreateReader();
                                valueReader.Read(buffer.CounterId, 0, innerIterator.CurrentKey.Size - parent.sizeOfGuid - sizeof(char));
                                DeleteCounterById(buffer.CounterId);

                                //we cannot delete keys while iterating, we save them and after iteration we delete them
                                keysToDelete.Add(new Tuple<Slice, Slice>(it.CurrentKey, innerIterator.CurrentKey));
                            } while (innerIterator.MoveNext() && --deletedTombstonesInBatch > 0);
                        }
                    } while (it.MovePrev() && --deletedTombstonesInBatch > 0);
                }
                keysToDelete.ForEach(x => dateToTombstones.MultiDelete(x.Item1, x.Item2));

                return true;
            }

            private Slice GetTombstoneSlice(TimeSpan tombstoneRetentionTime)
            {
                var timeAgo = DateTime.Now.AddTicks(-tombstoneRetentionTime.Ticks);
                EndianBitConverter.Big.CopyBytes(timeAgo.Ticks, buffer.TombstoneTicks.Value, 0);
                var tombstone = new Slice(buffer.TombstoneTicks.Value);
                return tombstone;
            }

            private void DeleteCounterById(byte[] counterIdBuffer)
            {
                var counterIdSlice = new Slice(counterIdBuffer);
                using (var it = counterIdWithNameToGroup.Iterate(false))
                {
                    it.RequiredPrefix = counterIdSlice;
                    var seek = it.Seek(it.RequiredPrefix);
                    Debug.Assert(seek == true);

                    var valueReader = it.CreateReaderForCurrent();
                    var groupNameSlice = valueReader.AsSlice();
                    var counterNameWithIdSlice = CreateCounterNameWithIdSlice(counterIdBuffer, it);
                    tombstonesGroupToCounters.MultiDelete(groupNameSlice, counterNameWithIdSlice);
                    counterIdWithNameToGroup.Delete(it.CurrentKey);
                }

                //remove all counters values for all servers
                var counterKeysToDelete = new List<Slice>();
                using (var it = counters.Iterate())
                {
                    it.RequiredPrefix = counterIdSlice;
                    if (it.Seek(it.RequiredPrefix) == false)
                        return;

                    do
                    {
                        var counterKey = it.CurrentKey;
                        RemoveOldEtagIfNeeded(counterKey);
                        countersToEtag.Delete(counterKey);
                        counterKeysToDelete.Add(counterKey);
                    } while (it.MoveNext());
                }
                counterKeysToDelete.ForEach(x => counters.Delete(x));
            }

            private Slice CreateCounterNameWithIdSlice(byte[] counterIdBuffer, TreeIterator it)
            {
                //{counter-id}{counter-name}
                var counterNameSize = it.CurrentKey.Size - sizeof(long);
                var keyReader = it.CurrentKey.CreateReader();
                //skipping the counter id
                keyReader.Skip(sizeof(long));
                int used;
                var counterNameBuffer = keyReader.ReadBytes(counterNameSize, out used);

                //needed structure: {counter-name}{counter-id}
                var counterNameWithIdSize = counterNameSize + sizeof(long);
                EnsureProperBufferSize(ref buffer.CounterNameWithId, counterNameWithIdSize);
                var sliceWriter = new SliceWriter(buffer.CounterNameWithId);
                sliceWriter.Write(counterNameBuffer, counterNameSize);
                sliceWriter.Write(counterIdBuffer);
                var counterNameWithIdSlice = sliceWriter.CreateSlice(counterNameWithIdSize);
                return counterNameWithIdSlice;
            }

            private static void EnsureProperBufferSize(ref byte[] buffer, int requiredBufferSize)
            {
                if (buffer.Length < requiredBufferSize)
                    buffer = new byte[Utils.NearestPowerOfTwo(requiredBufferSize)];
            }

            public void Commit()
            {
                transaction.Commit();
                parent.LastWrite = SystemTime.UtcNow;
                parent.Notify();
            }

            public void Dispose()
            {
                if (transaction != null)
                    transaction.Dispose();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            [Conditional("DEBUG")]
            private void ThrowIfDisposed()
            {
                if (transaction.IsDisposed)
                    throw new ObjectDisposedException("CounterStorage::Writer", "The writer should not be used after being disposed.");
            }
        }

        internal class CounterDetails
        {
            public Slice IdSlice { get; set; }
            public string Name { get; set; }
            public string Group { get; set; }
        }

        public class ServerEtag
        {
            public Guid ServerId { get; set; }
            public long Etag { get; set; }
        }

        public class ReplicationSourceInfo : ServerEtag
        {
            public string SourceName { get; set; }
        }

        private static class TreeNames
        {
            public const string ReplicationSources = "servers->sourceName";
            public const string ServersLastEtag = "servers->lastEtag";
            public const string Counters = "counters";
            public const string DateToTombstones = "date->tombstones";
            public const string GroupToCounters = "group->counters";
            public const string TombstonesGroupToCounters = "tombstones-group->counters";
            public const string CounterIdWithNameToGroup = "counterIdWithName->group";
            public const string CountersToEtag = "counters->etags";
            public const string EtagsToCounters = "etags->counters";
            public const string Metadata = "$metadata";
        }
    }
}
