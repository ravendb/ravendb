using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Counters.Notifications;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Config;
using Raven.Database.Counters.Controllers;
using Raven.Database.Counters.Notifications;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Connections;
using Raven.Database.Util;
using Voron;
using Voron.Impl;
using Voron.Trees;
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
		private readonly BufferPool bufferPool;
		private readonly JsonSerializer jsonSerializer;

		private long lastEtag;
		public event Action CounterUpdated = () => { };

		public string CounterStorageUrl { get; private set; }

		public DateTime LastWrite { get; private set; }

		public Guid ServerId { get; private set; }

		public string Name { get; private set; }

		public string ResourceName { get; private set; }

		public int ReplicationTimeoutInMs { get; private set; }

		public CounterStorage(string serverUrl, string storageName, InMemoryRavenConfiguration configuration, TransportState recievedTransportState = null)
		{			
			CounterStorageUrl = string.Format("{0}cs/{1}", serverUrl, storageName);
			Name = storageName;
			ResourceName = string.Concat(Constants.Counter.UrlPrefix, "/", storageName);

			var options = configuration.RunInMemory ? StorageEnvironmentOptions.CreateMemoryOnly()
				: CreateStorageOptionsFromConfiguration(configuration.Counter.DataDirectory, configuration.Settings);

			storageEnvironment = new StorageEnvironment(options);
			transportState = recievedTransportState ?? new TransportState();
			notificationPublisher = new NotificationPublisher(transportState);
			replicationTask = new ReplicationTask(this);
			ReplicationTimeoutInMs = configuration.Replication.ReplicationRequestTimeoutInMilliseconds;
			metricsCounters = new CountersMetricsManager();
			Configuration = configuration;
			ExtensionsState = new AtomicDictionary<object>();
			jsonSerializer = new JsonSerializer();
			bufferPool = new BufferPool(1024, Int32.MaxValue);

			Initialize();
		}

		private void Initialize()
		{
			using (var tx = CounterStorageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
			{
				storageEnvironment.CreateTree(tx, TreeNames.ServersLastEtag);
				storageEnvironment.CreateTree(tx, TreeNames.Counters);
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

				replicationTask.StartReplication();
			
				tx.Commit();
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

		private BufferPool BufferPool
		{
			get { return bufferPool; }
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

				RequestsDuration = metrics.RequestDuationMetric.CreateHistogramData(),
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
			bufferPool.Dispose();
			var exceptionAggregator = new ExceptionAggregator(Log, "Could not properly dispose of CounterStorage: " + Name);

			if (replicationTask != null)
				exceptionAggregator.Execute(replicationTask.Dispose);

			if (CounterStorageEnvironment != null)
				exceptionAggregator.Execute(CounterStorageEnvironment.Dispose);

			if (metricsCounters != null)
				exceptionAggregator.Execute(metricsCounters.Dispose);

			exceptionAggregator.ThrowIfNeeded();
			storageEnvironment.Dispose();
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
						//the last byte contains the sign
						//we consistently use utf8 encoding in the system, 
						//thats why single character will be one-byte width

						var signByte = it.CurrentKey[it.CurrentKey.Size - 1];
						var value = it.CreateReaderForCurrent().ReadLittleEndianInt64();
						if (Convert.ToChar(signByte) == ValueSign.Positive)
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
					var buffer = parent.BufferPool.TakeBuffer(sizeof(long));

					try
					{
						EndianBitConverter.Little.CopyBytes(etag, buffer, 0);
						var slice = new Slice(buffer);
						if (it.Seek(slice) == false)
							yield break;
						do
						{
							var currentDataSize = it.GetCurrentDataSize();

							if (buffer.Length < currentDataSize)
							{
								parent.BufferPool.ReturnBuffer(buffer);
								buffer = parent.BufferPool.TakeBuffer(currentDataSize);
							}

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
					finally
					{
						parent.BufferPool.ReturnBuffer(buffer);
					}
				}
			}

			public IEnumerable<ServerEtag> GetServerEtags()
			{
				var buffer = parent.BufferPool.TakeBuffer(sizeof(long));
				try
				{
					using (var it = serversLastEtag.Iterate())
					{
						if (it.Seek(Slice.BeforeAllKeys) == false)
							yield break;
						do
						{
							//should never ever happen :)
							Debug.Assert(buffer.Length >= it.GetCurrentDataSize());

							it.CreateReaderForCurrent().Read(buffer, 0, buffer.Length);
							yield return new ServerEtag
							{
								ServerId = Guid.Parse(it.CurrentKey.ToString()),
								Etag = EndianBitConverter.Little.ToInt64(buffer, 0),
							};

						} while (it.MoveNext());
					}
				}
				finally
				{
					parent.BufferPool.ReturnBuffer(buffer);
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
			private readonly Tree counters, serversLastEtag, etagsToCounters, countersToEtag, countersGroups, groupAndCounterName, metadata;
			private readonly byte[] etagBuffer;

			public Writer(CounterStorage parent, Transaction transaction)
			{
				if (transaction.Flags != TransactionFlags.ReadWrite) //precaution
					throw new InvalidOperationException(string.Format("Counters writer cannot be created with read-only transaction. (tx id = {0})", transaction.Id));

				this.parent = parent;
				this.transaction = transaction;
				reader = new Reader(parent, transaction);
				serversLastEtag = transaction.State.GetTree(transaction, TreeNames.ServersLastEtag);
				counters = transaction.State.GetTree(transaction, TreeNames.Counters);
				countersGroups = transaction.State.GetTree(transaction, TreeNames.CountersGroups);
				countersToEtag = transaction.State.GetTree(transaction, TreeNames.CountersToEtag);
				etagsToCounters = transaction.State.GetTree(transaction, TreeNames.EtagsToCounters);
				groupAndCounterName = transaction.State.GetTree(transaction, TreeNames.GroupAndCounterName);
				metadata = transaction.State.GetTree(transaction, TreeNames.Metadata);
				etagBuffer = parent.BufferPool.TakeBuffer(sizeof (long));
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
				var doesCounterExist = Store(groupName, counterName, parent.ServerId, sign, counterKey =>
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
				var doesCounterExist = Store(counterValue.Group(), counterValue.CounterName(), counterValue.ServerId(), sign, counterKey =>
				{
					var sliceWriter = new SliceWriter(parent.BufferPool.TakeBuffer(sizeof(long)));
					try
					{
						sliceWriter.Write(counterValue.Value);
						counters.Add(counterKey, sliceWriter.CreateSlice());
					}
					finally
					{
						parent.BufferPool.ReturnBuffer(sliceWriter.Buffer);
					}
				});

				if (doesCounterExist)
					return counterValue.Value >= 0 ? CounterChangeAction.Increment : CounterChangeAction.Decrement;

				return CounterChangeAction.Add;
			}

			// full counter name: foo/bar/server-id/+
			private unsafe bool Store(string groupName, string counterName, Guid serverId, char sign, Action<Slice> storeAction)
			{
				var groupSize = Encoding.UTF8.GetByteCount(groupName);
				var counterNameSize = Encoding.UTF8.GetByteCount(counterName);
				var fullCounterNameSize = groupSize + 
										  (sizeof(byte) * 3) + 
										  counterNameSize + 
									      sizeof(Guid) + 
										  sizeof(char);
				
				var sliceWriter = GetFullCounterNameAsSliceWriter(groupName,
					counterName,
					serverId,
					sign,
					fullCounterNameSize);

				try
				{
					var groupWithCounterName = sliceWriter.CreateSlice(groupSize + counterNameSize + (2*sizeof (byte)));
					var doesCounterExist = CreateCounterGroupIfNeeded(groupWithCounterName, sliceWriter, groupSize);

					groupAndCounterName.Add(groupWithCounterName, new byte[0]);

					//save counter full name and its value into the counters tree
					var counterKey = sliceWriter.CreateSlice();

					storeAction(counterKey);

					RemoveOldEtagIfNeeded(counterKey);
					UpdateCounterMetadata(counterKey);

					return doesCounterExist;
				}
				finally
				{
					parent.BufferPool.ReturnBuffer(sliceWriter.Buffer);
				}
			}

			private void UpdateCounterMetadata(Slice counterKey)
			{
				parent.lastEtag++;
				EndianBitConverter.Little.CopyBytes(parent.lastEtag, etagBuffer, 0);
				var newEtagSlice = new Slice(etagBuffer);
				etagsToCounters.Add(newEtagSlice, counterKey);
				countersToEtag.Add(counterKey, newEtagSlice);
			}

			private void RemoveOldEtagIfNeeded(Slice counterKey)
			{
				var readResult = countersToEtag.Read(counterKey);
				if (readResult != null) // remove old etag entry
				{
					readResult.Reader.Read(etagBuffer, 0, sizeof (long));
					var oldEtagSlice = new Slice(etagBuffer);
					etagsToCounters.Delete(oldEtagSlice);
				}
			}

			private bool CreateCounterGroupIfNeeded(Slice groupWithCounterName, SliceWriter sliceWriter, int groupSize)
			{
				var doesCounterExist = DoesCounterExist(groupWithCounterName);
				if (doesCounterExist == false)
				{
					//it's a new counter in the group
					var groupKey = sliceWriter.CreateSlice(groupSize);
					countersGroups.Increment(groupKey, 1);
				}
				return doesCounterExist;
			}

			private SliceWriter GetFullCounterNameAsSliceWriter(string groupName, string counterName, Guid serverId, char sign, int fullCounterNameSize)
			{
				var sliceWriter = new SliceWriter(parent.BufferPool.TakeBuffer(fullCounterNameSize));				
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
					return CounterChangeAction.None;

				var difference = CalculateOverallTotal(counterValuesByPrefix);
				if (difference != 0)
				{
					difference = -difference;
					var counterChangeAction = Store(groupName, counterName, difference);
					return counterChangeAction;
				}
				return CounterChangeAction.None;
			}

			public void RecordLastEtagFor(Guid serverId, long lastEtag)
			{
				serversLastEtag.Add(serverId.ToString(), EndianBitConverter.Little.GetBytes(lastEtag));
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
				using (var it = counters.Iterate())
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
				parent.BufferPool.ReturnBuffer(etagBuffer);
				parent.LastWrite = SystemTime.UtcNow;
				if (transaction != null)
					transaction.Dispose();
			}
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
			public const string CountersGroups = "groups";
			public const string CountersToEtag = "counters->etags";
			public const string EtagsToCounters = "etags->counters";
			public const string GroupAndCounterName = "groupAndCounterName";
			public const string Metadata = "$metadata";
		}
	}
}