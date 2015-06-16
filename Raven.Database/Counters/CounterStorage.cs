using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Mono.CSharp;
using Raven.Abstractions;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Counters.Notifications;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Config;
using Raven.Database.Counters.Controllers;
using Raven.Database.Counters.Notifications;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Server.Abstractions;
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

		private long lastEtag;
		public event Action CounterUpdated = () => { };

		public string CounterStorageUrl { get; private set; }

		public DateTime LastWrite { get; private set; }

		public Guid ServerId { get; private set; }

		public Guid TombstoneId { get; private set; }

		public string Name { get; private set; }

		public string ResourceName { get; private set; }

		public int ReplicationTimeoutInMs { get; private set; }

		public CounterStorage(string serverUrl, string storageName, InMemoryRavenConfiguration configuration, TransportState receivedTransportState = null)
		{			
			CounterStorageUrl = string.Format("{0}cs/{1}", serverUrl, storageName);
			Name = storageName;
			ResourceName = string.Concat(Constants.Counter.UrlPrefix, "/", storageName);

			var options = configuration.RunInMemory ? StorageEnvironmentOptions.CreateMemoryOnly()
				: CreateStorageOptionsFromConfiguration(configuration.Counter.DataDirectory, configuration.Settings);

			storageEnvironment = new StorageEnvironment(options);
			transportState = receivedTransportState ?? new TransportState();
			notificationPublisher = new NotificationPublisher(transportState);
			replicationTask = new ReplicationTask(this);
			ReplicationTimeoutInMs = configuration.Replication.ReplicationRequestTimeoutInMilliseconds;
			metricsCounters = new CountersMetricsManager();
			Configuration = configuration;
			ExtensionsState = new AtomicDictionary<object>();
			jsonSerializer = new JsonSerializer();

			//TODO: generate deterministic guid
			TombstoneId = new Guid("00000000-0000-0000-0000-000000000000");
			Initialize();
		}

		private void Initialize()
		{
			using (var tx = CounterStorageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
			{
				storageEnvironment.CreateTree(tx, TreeNames.ServersLastEtag);
				storageEnvironment.CreateTree(tx, TreeNames.Counters);
				storageEnvironment.CreateTree(tx, TreeNames.Tombstones);
				storageEnvironment.CreateTree(tx, TreeNames.CountersGroups);
				storageEnvironment.CreateTree(tx, TreeNames.CountersToEtag);
				storageEnvironment.CreateTree(tx, TreeNames.GroupAndCounterName);
				
				var etags = CounterStorageEnvironment.CreateTree(tx, TreeNames.EtagsToCounters);
				var metadata = CounterStorageEnvironment.CreateTree(tx, TreeNames.Metadata);
				var id = metadata.Read("id");

				if (id == null) // new counter db
				{
					ServerId = Guid.NewGuid();
					var serverIdBytes = ServerId.ToByteArray();

					metadata.Add("id", serverIdBytes);

				}
				else // existing counter db
				{
					int used;
					ServerId = new Guid(id.Reader.ReadBytes(16, out used));

					using (var it = etags.Iterate())
					{
						if (it.Seek(Slice.AfterAllKeys))
						{
							lastEtag = it.CurrentKey.CreateReader().ReadLittleEndianInt64();
						}
					}
				}

				tx.Commit();

				replicationTask.StartReplication();
			}
		}

		string IResourceStore.Name
		{
			get { return Name; }
		}

		[CLSCompliant(false)]
		public CountersMetricsManager MetricsCounters
		{
			get { return metricsCounters; }
		}

		public TransportState TransportState
		{
			get { return transportState; }
		}

		public NotificationPublisher Publisher
		{
			get { return notificationPublisher; }
		}

		public ReplicationTask ReplicationTask
		{
			get { return replicationTask; }
		}

		public StorageEnvironment CounterStorageEnvironment
		{
			get { return storageEnvironment; }
		}

		private JsonSerializer JsonSerializer
		{
			get { return jsonSerializer; }
		}

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
					LastCounterEtag = lastEtag,
					ReplicationTasksCount = replicationTask.GetActiveTasksCount(),
					CounterStorageSize = SizeHelper.Humane(CounterStorageEnvironment.Stats().UsedDataFileSizeInBytes),
					ReplicatedServersCount = 0, //TODO: get the correct number
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
				return StorageEnvironmentOptions.CreateMemoryOnly();

			bool allowIncrementalBackupsSetting;
			if (Boolean.TryParse(settings[Constants.Voron.AllowIncrementalBackups] ?? "false", out allowIncrementalBackupsSetting) == false)
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
			return new Reader(this, CounterStorageEnvironment.NewTransaction(TransactionFlags.Read));
		}

		[CLSCompliant(false)]
		public Writer CreateWriter()
		{
			return new Writer(this, CounterStorageEnvironment.NewTransaction(TransactionFlags.ReadWrite));
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

			exceptionAggregator.ThrowIfNeeded();
		}

		[CLSCompliant(false)]
		public class Reader : IDisposable
		{
			private readonly Transaction transaction;
			private readonly Tree serversLastEtag, counters, countersToEtags, countersGroups, etagsToCounters, groupAndCounterName, metadata;
			private readonly CounterStorage parent;

			[CLSCompliant(false)]
			public Reader(CounterStorage parent, Transaction transaction)
			{
				this.transaction = transaction;
				this.parent = parent;
				serversLastEtag = transaction.State.GetTree(transaction, TreeNames.ServersLastEtag);
				counters = transaction.State.GetTree(transaction, TreeNames.Counters);
				countersGroups = transaction.State.GetTree(transaction, TreeNames.CountersGroups);
				countersToEtags = transaction.State.GetTree(transaction, TreeNames.CountersToEtag);
				etagsToCounters = transaction.State.GetTree(transaction, TreeNames.EtagsToCounters);
				groupAndCounterName = transaction.State.GetTree(transaction, TreeNames.GroupAndCounterName);
				metadata = transaction.State.GetTree(transaction, TreeNames.Metadata);
			}

			public long GetCountersCount()
			{
				return groupAndCounterName.State.EntriesCount;
			}

			public long GetGroupsCount()
			{
				return countersGroups.State.EntriesCount;
			}

			public bool CounterExists(string group, string counterName)
			{
				var slice = MergeGroupAndName(group, counterName);
				using (var it = groupAndCounterName.Iterate())
				{
					it.RequiredPrefix = slice;
					return it.Seek(slice);
				}
			}

			public IEnumerable<string> GetCountersByPrefixes(string groupsPrefix, int skip = 0, int take = Int32.MaxValue)
			{
				Debug.Assert(take > 0);
				Debug.Assert(skip >= 0);

				using (var it = groupAndCounterName.Iterate())
				{
					it.RequiredPrefix = groupsPrefix;
					if (it.Seek(it.RequiredPrefix) == false || it.Skip(skip) == false)
						yield break;

					do
					{
						yield return it.CurrentKey.ToString();
					} while (it.MoveNext() && --take > 0);
				}
			}

			public long GetCounterTotalValue(string groupName, string counterName)
			{
				var groupWithCounterName = string.Concat(groupName, Constants.Counter.Separator, counterName, Constants.Counter.Separator);
				return GetCounterSummary(groupWithCounterName).Total;
			}

			//example: group/counterName/
			public CounterSummary GetCounterSummary(string groupWithCounterName)
			{
				//precaution, verify that groupWithCounterName is in form of [group]/[counter]/
				Debug.Assert(groupWithCounterName.Count(@char => @char == '/') == 2);

				var counterSummary = new CounterSummary();
				var splittedName = groupWithCounterName.Split('/');
				counterSummary.Group = splittedName[0];
				counterSummary.CounterName = splittedName[1];

				using (var it = counters.Iterate())
				{
					it.RequiredPrefix = groupWithCounterName;
					if (it.Seek(it.RequiredPrefix) == false)
						return counterSummary;

					do
					{
						/*//the last byte contains the sign
						//we consistently use utf8 encoding in the system, 
						//thats why single character will be one-byte width*/

						var counterValue = new CounterValue(it.CurrentKey.ToString(), 0);
						if (counterValue.ServerId().Equals(parent.TombstoneId))
							throw new Exception(string.Format("Counter was already deleted. Group: {0}, Counter Name: {1}", counterSummary.Group, counterSummary.CounterName));

						var value = it.CreateReaderForCurrent().ReadLittleEndianInt64();
						if (counterValue.IsPositive())
							counterSummary.Increments += value;
						else
							counterSummary.Decrements += value;
					} while (it.MoveNext());

					return counterSummary;
				}
			}

			public IEnumerable<string> GetFullCounterNames(string prefix)
			{
				using (var it = counters.Iterate())
				{
					it.RequiredPrefix = prefix;
					if (it.Seek(it.RequiredPrefix) == false)
						yield break;
					do
					{						
						yield return it.CurrentKey.ToString();
					} while (it.MoveNext());
				}
			}

			public IEnumerable<CounterGroup> GetCounterGroups()
			{
				using (var it = countersGroups.Iterate())
				{
					if (it.Seek(Slice.BeforeAllKeys) == false)
						yield break;
					do
					{
						yield return new CounterGroup
						{
							Name = it.CurrentKey.ToString(),
							Count = it.CreateReaderForCurrent().ReadLittleEndianInt64()
						};
					} while (it.MoveNext());
				}
			}

			public long GetCounterValue(string fullCounterName)
			{
				var readResult = counters.Read(fullCounterName);
				if (readResult == null)
					return -1;

				return readResult.Reader.ReadLittleEndianInt64();
			}

			public long? GetCounterOverallTotal(string groupName, string counterName)
			{
				var counterValues = GetCounterValuesByPrefix(groupName, counterName);
				if (counterValues == null)
					return null;

				return CalculateOverallTotal(counterValues);
			}

			public Counter GetCounterValuesByPrefix(string groupName, string counterName)
			{
				return GetCounterValuesByPrefix(MergeGroupAndName(groupName, counterName).ToString());
			}

			//namePrefix: foo/bar/
			public Counter GetCounterValuesByPrefix(string namePrefix)
			{
				using (var it = counters.Iterate())
				{
					it.RequiredPrefix = namePrefix;
					if (it.Seek(namePrefix) == false)
						return null;

					var result = new Counter();
					do
					{
						result.CounterValues.Add(new CounterValue
						(
							it.CurrentKey.ToString(),
							it.CreateReaderForCurrent().ReadLittleEndianInt64()
						));
					} while (it.MoveNext());
					return result;
				}
			}

			public IEnumerable<ReplicationCounter> GetCountersSinceEtag(long etag)
			{
				using (var it = etagsToCounters.Iterate())
				{
					var buffer = new byte[sizeof(long)];
					EndianBitConverter.Little.CopyBytes(etag, buffer, 0);
					var slice = new Slice(buffer);
					if (it.Seek(slice) == false)
						yield break;
					do
					{
						var currentDataSize = it.GetCurrentDataSize();
						EnsureBufferSize(ref buffer, currentDataSize);

						it.CreateReaderForCurrent().Read(buffer, 0, currentDataSize);
						var fullCounterName = Encoding.UTF8.GetString(buffer, 0, currentDataSize);

						var etagResult = countersToEtags.Read(fullCounterName);
						var counterEtag = etagResult == null ? 0 : etagResult.Reader.ReadLittleEndianInt64();

						yield return new ReplicationCounter
						{
							FullCounterName = fullCounterName,
							Value = GetCounterValue(fullCounterName),
							Etag = counterEtag
						};
					} while (it.MoveNext());
				}
			}

			public IEnumerable<ServerEtag> GetServerEtags()
			{
				using (var it = serversLastEtag.Iterate())
				{
					if (it.Seek(Slice.BeforeAllKeys) == false)
						yield break;
					do
					{
						//should never ever happen :)
						/*Debug.Assert(buffer.Length >= it.GetCurrentDataSize());

						it.CreateReaderForCurrent().Read(buffer, 0, buffer.Length);*/
						yield return new ServerEtag
						{
							ServerId = Guid.Parse(it.CurrentKey.ToString()),
							Etag = it.CreateReaderForCurrent().ReadLittleEndianInt64()
						};

					} while (it.MoveNext());
				}
			}

			public long GetLastEtagFor(Guid serverId)
			{
				var lastEtagBytes = serversLastEtag.Read(serverId.ToString()); 
				return lastEtagBytes != null ? lastEtagBytes.Reader.ReadLittleEndianInt64() : 0;
			}

			public CountersReplicationDocument GetReplicationData()
			{
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
			private readonly Tree counters, tombstones, serversLastEtag, etagsToCounters, countersToEtag, countersGroups, groupAndCounterName, metadata;

			private static class Buffer
			{
				public readonly static byte[] Etag = new byte[sizeof(long)];
				public readonly static byte[] CounterValue = new byte[sizeof(long)];
				public static byte[] CounterName = new byte[0];
				public static long c = DateTime.MaxValue.Ticks;
			}

			public Writer(CounterStorage parent, Transaction transaction)
			{
				if (transaction.Flags != TransactionFlags.ReadWrite) //precaution
					throw new InvalidOperationException(string.Format("Counters writer cannot be created with read-only transaction. (tx id = {0})", transaction.Id));

				this.parent = parent;
				this.transaction = transaction;
				reader = new Reader(parent, transaction);
				serversLastEtag = transaction.State.GetTree(transaction, TreeNames.ServersLastEtag);
				counters = transaction.State.GetTree(transaction, TreeNames.Counters);
				tombstones = transaction.State.GetTree(transaction, TreeNames.Tombstones);
				countersGroups = transaction.State.GetTree(transaction, TreeNames.CountersGroups);
				countersToEtag = transaction.State.GetTree(transaction, TreeNames.CountersToEtag);
				etagsToCounters = transaction.State.GetTree(transaction, TreeNames.EtagsToCounters);
				groupAndCounterName = transaction.State.GetTree(transaction, TreeNames.GroupAndCounterName);
				metadata = transaction.State.GetTree(transaction, TreeNames.Metadata);
			}

			public long GetCounterValue(string fullCounterName)
			{
				return reader.GetCounterValue(fullCounterName);
			}

			private Counter GetCounterValuesByPrefix(string groupName, string counterName)
			{
				return reader.GetCounterValuesByPrefix(groupName, counterName);
			}

			public long GetLastEtagFor(Guid serverId)
			{
				return reader.GetLastEtagFor(serverId);
			}

			public CounterChangeAction Store(string groupName, string counterName, long delta)
			{
				var sign = delta >= 0 ? ValueSign.Positive : ValueSign.Negative;
				var doesCounterExist = Store(groupName, counterName, parent.ServerId, sign, (counterKey, groupAndCounterNameSlice) =>
				{
					if (sign == ValueSign.Negative)
						delta = -delta;
					counters.Increment(counterKey, delta);
				});

				if (doesCounterExist)
					return sign == ValueSign.Positive ? CounterChangeAction.Increment : CounterChangeAction.Decrement;

				return CounterChangeAction.Add;
			}

			public CounterChangeAction Store(CounterValue counterValue)
			{
				var sign = counterValue.IsPositive() ? ValueSign.Positive : ValueSign.Negative;
				var serverId = counterValue.ServerId();
				var doesCounterExist = Store(counterValue.Group(), counterValue.CounterName(), serverId, sign, (counterKey, groupAndCounterNameSlice) =>
				{
					EndianBitConverter.Little.CopyBytes(counterValue.Value, Buffer.CounterValue, 0);
					var counterValueSlice = new Slice(Buffer.CounterValue);
					counters.Add(counterKey, counterValueSlice);

					if (serverId == parent.TombstoneId)
						tombstones.Add(counterValueSlice, groupAndCounterNameSlice);
				});

				if (doesCounterExist)
					return counterValue.Value >= 0 ? CounterChangeAction.Increment : CounterChangeAction.Decrement;

				return CounterChangeAction.Add;
			}

			// full counter name: foo/bar/server-id/+
			private bool Store(string groupName, string counterName, Guid serverId, string sign, Action<Slice, Slice> storeAction)
			{
				var groupSize = Encoding.UTF8.GetByteCount(groupName);
				var counterNameSize = Encoding.UTF8.GetByteCount(counterName);
				var fullCounterNameSize = groupSize + 
										  (sizeof(byte) * 3) + 
										  counterNameSize + 
									      32 + 
										  sizeof(byte);

				var sliceWriter = GetFullCounterNameAsSliceWriter(groupName,
					counterName,
					serverId,
					sign,
					fullCounterNameSize);

				var groupAndCounterNameSlice = sliceWriter.CreateSlice(groupSize + counterNameSize + (2*sizeof (byte)));
				var doesCounterExist = DoesCounterExist(groupAndCounterNameSlice);
				var groupKey = sliceWriter.CreateSlice(groupSize);
				if (serverId == parent.TombstoneId)
				{
					//if it's a tombstone, we can remove it from the groups and groupAndCounterName trees
					var readResult = countersGroups.Read(groupKey);
					if (readResult != null)
					{
						if (readResult.Reader.ReadLittleEndianInt64() == 1)
							countersGroups.Delete(groupKey);
						else
							countersGroups.Increment(groupKey, -1);	
					}
					groupAndCounterName.Delete(groupAndCounterNameSlice);
				}
				else if (doesCounterExist == false)
				{
					//TODO: update tombstone
					//if the counter doesn't exist we need to update the appropriate trees
					countersGroups.Increment(groupKey, 1);
					groupAndCounterName.Add(groupAndCounterNameSlice, new byte[0]);
				}

				//save counter full name and its value into the counters tree
				var counterKey = sliceWriter.CreateSlice();
				storeAction(counterKey, groupAndCounterNameSlice);

				RemoveOldEtagIfNeeded(counterKey);
				UpdateCounterMetadata(counterKey);

				return doesCounterExist;
			}

			private void UpdateCounterMetadata(Slice counterKey)
			{
				parent.lastEtag++;
				EndianBitConverter.Little.CopyBytes(parent.lastEtag, Buffer.Etag, 0);
				var newEtagSlice = new Slice(Buffer.Etag);
				etagsToCounters.Add(newEtagSlice, counterKey);
				countersToEtag.Add(counterKey, newEtagSlice);
			}

			private void RemoveOldEtagIfNeeded(Slice counterKey)
			{
				var readResult = countersToEtag.Read(counterKey);
				if (readResult != null) // remove old etag entry
				{
					readResult.Reader.Read(Buffer.Etag, 0, sizeof(long));
					var oldEtagSlice = new Slice(Buffer.Etag);
					etagsToCounters.Delete(oldEtagSlice);
				}
			}

			private static SliceWriter GetFullCounterNameAsSliceWriter(string groupName, string counterName, Guid serverId, string sign, int fullCounterNameSize)
			{
				EnsureBufferSize(ref Buffer.CounterName, fullCounterNameSize);
				//TODO: move to Big Endian
				var sliceWriter = new SliceWriter(Buffer.CounterName);				
				sliceWriter.Write(groupName);
				
				sliceWriter.Write(Constants.Counter.Separator);
				sliceWriter.Write(counterName);
				sliceWriter.Write(Constants.Counter.Separator);
				sliceWriter.Write(serverId.ToString());
				sliceWriter.Write(Constants.Counter.Separator);
				sliceWriter.Write(sign);
				return sliceWriter;
			}

			public CounterChangeAction Reset(string groupName, string counterName)
			{
				var counterValuesByPrefix = GetCounterValuesByPrefix(groupName, counterName);
				if (counterValuesByPrefix == null)
					throw new InvalidOperationException(string.Format("Counter doesn't exist. Group: {0}, Counter Name: {1}", groupName, counterName));

				var difference = CalculateOverallTotal(counterValuesByPrefix);
				if (difference == 0)
					return CounterChangeAction.None;

				difference = -difference;
				var counterChangeAction = Store(groupName, counterName, difference);
				return counterChangeAction;
			}

			public void Delete(string groupName, string counterName)
			{
				var groupAndCounterNameSlice = MergeGroupAndName(groupName, counterName);
				var counterExists = DoesCounterExist(groupAndCounterNameSlice);
				if (counterExists == false)
					throw new InvalidOperationException(string.Format("Counter doesn't exist. Group: {0}, Counter Name: {1}", groupName, counterName));

				/*Store(groupName, counterName, parent.TombstoneId, ValueSign.Positive, counterKey =>
				{
					EndianBitConverter.Big.CopyBytes(DateTime.Now.Ticks, Buffer.CounterValue, 0);
					var slice = new Slice(Buffer.CounterValue);
					counters.Add(counterKey, slice);

					tombstones.Add(slice, groupAndCounterNameSlice);
				});*/
				//TODO: tombstones cleanup, older than two weeks

			}

			public void RecordLastEtagFor(Guid serverId, long lastEtag)
			{
				EndianBitConverter.Big.CopyBytes(lastEtag, Buffer.Etag, 0);
				var slice = new Slice(Buffer.Etag);
				serversLastEtag.Add(serverId.ToString(), slice);
			}

			public void UpdateReplications(CountersReplicationDocument newReplicationDocument)
			{
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

			private bool DoesCounterExist(Slice groupWithCounterName)
			{
				using (var it = groupAndCounterName.Iterate())
				{
					it.RequiredPrefix = groupWithCounterName;
					return it.Seek(groupWithCounterName);
				}
			}

			public void Commit(bool notifyParent = true)
			{
				transaction.Commit();
				parent.LastWrite = SystemTime.UtcNow;
				if (notifyParent)
				{
					parent.Notify();
				}
			}

			public void Dispose()
			{
				parent.LastWrite = SystemTime.UtcNow;
				if (transaction != null)
					transaction.Dispose();
			}
		}

		private static void EnsureBufferSize(ref byte[] buffer, int requiredBufferSize)
		{
			if (buffer.Length < requiredBufferSize)
				buffer = new byte[Utils.NearestPowerOfTwo(requiredBufferSize)];
		}

		private static long CalculateOverallTotal(Counter counterValuesByPrefix)
		{
			long sum = 0;
			// ReSharper disable once LoopCanBeConvertedToQuery
			foreach (var x in counterValuesByPrefix.CounterValues)
				sum += x.IsPositive() ? x.Value : -x.Value;
			return sum;
		}

		private static Slice MergeGroupAndName(string group, string counterName)
		{
			var groupSize = Encoding.UTF8.GetByteCount(group);
			var counterNameSize = Encoding.UTF8.GetByteCount(counterName);
			var sliceWriter = new SliceWriter(groupSize + counterNameSize + (sizeof(byte) * 2));
			sliceWriter.Write(group);
			sliceWriter.Write(Constants.Counter.Separator);
			sliceWriter.Write(counterName);
			sliceWriter.Write(Constants.Counter.Separator);
			return sliceWriter.CreateSlice();
		}

		public class ServerEtag
		{
			public Guid ServerId { get; set; }
			public long Etag { get; set; }
		}

		private static class TreeNames
		{
			public const string ServersLastEtag = "servers->lastEtag";
			public const string Counters = "counters";
			public const string Tombstones = "tombstones";
			public const string CountersGroups = "groups";
			public const string CountersToEtag = "counters->etags";
			public const string EtagsToCounters = "etags->counters";
			public const string GroupAndCounterName = "groupAndCounterName";
			public const string Metadata = "$metadata";
		}
	}
}