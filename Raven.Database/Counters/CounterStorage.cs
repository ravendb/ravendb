using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
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
			Initialize();
		}


		private void Initialize()
		{
			using (var tx = CounterStorageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
			{
				storageEnvironment.CreateTree(tx, "servers->lastEtag");
				storageEnvironment.CreateTree(tx, "counters");
				storageEnvironment.CreateTree(tx, "groups");
				storageEnvironment.CreateTree(tx, "counters->etags");
				storageEnvironment.CreateTree(tx, "groupAndCounterName");
				
				var etags = CounterStorageEnvironment.CreateTree(tx, "etags->counters");
				var metadata = CounterStorageEnvironment.CreateTree(tx, "$metadata");
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
			return new Reader(CounterStorageEnvironment.NewTransaction(TransactionFlags.Read));
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
			private readonly byte[] serverIdBytes = new byte[16];

			[CLSCompliant(false)]
			public Reader(Transaction transaction)
			{
				this.transaction = transaction;
				serversLastEtag = transaction.State.GetTree(transaction, "servers->lastEtag");
				counters = transaction.State.GetTree(transaction, "counters");
				countersGroups = transaction.State.GetTree(transaction, "groups");
				countersToEtags = transaction.State.GetTree(transaction, "counters->etags");
				etagsToCounters = transaction.State.GetTree(transaction, "etags->counters");
				groupAndCounterName = transaction.State.GetTree(transaction, "groupAndCounterName");
				metadata = transaction.State.GetTree(transaction, "$metadata");
			}

			public long GetCountersCount()
			{
				return countersToEtags.State.EntriesCount;
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

			public IEnumerable<string> GetCountersByPrefixes(string groupsPrefix, int skip, int take)
			{
				using (var it = groupAndCounterName.Iterate())
				{
					it.RequiredPrefix = groupsPrefix;
					it.Skip(skip);
					if (it.Seek(it.RequiredPrefix) == false)
						yield break;

					do
					{
						yield return it.CurrentKey.ToString();
					} while (it.MoveNext() && --take > 0);
				}
			}

			//example: group/counterName/
			public CounterSummary GetCounterSummary(string groupWithCoutnerName)
			{
				var counterSummary = new CounterSummary();
				var splittedName = groupWithCoutnerName.Split('/');
				counterSummary.Group = splittedName[0];
				counterSummary.CounterName = splittedName[1];

				using (var it = counters.Iterate())
				{
					it.RequiredPrefix = groupWithCoutnerName;
					if (it.Seek(it.RequiredPrefix) == false)
						return counterSummary;

					do
					{
						//the last byte containes the sign
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
					var buffer = new byte[sizeof(long)];
					EndianBitConverter.Little.CopyBytes(etag, buffer, 0);
					var slice = new Slice(buffer);
					if (it.Seek(slice) == false)
						yield break;
					do
					{
						var currentDataSize = it.GetCurrentDataSize();

						if (buffer.Length < currentDataSize)
						{
							buffer = new byte[Utils.NearestPowerOfTwo(currentDataSize)];
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
			}

			public IEnumerable<ServerEtag> GetServerEtags()
			{
				var buffer = new byte[sizeof(long)];
				using (var it = serversLastEtag.Iterate())
				{
					if (it.Seek(Slice.BeforeAllKeys) == false)
						yield break;
					do
					{
						if (buffer.Length < it.GetCurrentDataSize())
						{
							buffer = new byte[Utils.NearestPowerOfTwo(it.GetCurrentDataSize())];
						}

						it.CurrentKey.CopyTo(0, serverIdBytes, 0, 16);
						it.CreateReaderForCurrent().Read(buffer, 0, buffer.Length);
						yield return new ServerEtag
						{
							ServerId = EndianBitConverter.ToString(serverIdBytes),
							Etag = EndianBitConverter.Little.ToInt64(buffer, 0),
						};

					} while (it.MoveNext());
				}
			}

			public long GetLastEtagFor(Guid serverId)
			{
				//var sliceWriter = new SliceWriter();
				var lastEtagBytes = serversLastEtag.Read(serverId.ToString()); //todo: consider changing that
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
			//private readonly byte[] tempThrowawayBuffer = new byte[sizeof(long)];
			private byte[] fullCounterNameBuffer = new byte[0];
			private readonly byte[] etagBuffer = new byte[sizeof(long)];

			public Writer(CounterStorage parent, Transaction transaction)
			{
				if (transaction.Flags != TransactionFlags.ReadWrite) //precaution
					throw new InvalidOperationException(string.Format("Counters writer cannot be created with read-only transaction. (tx id = {0})", transaction.Id));

				this.parent = parent;
				this.transaction = transaction;
				reader = new Reader(transaction);
				serversLastEtag = transaction.State.GetTree(transaction, "servers->lastEtag");
				counters = transaction.State.GetTree(transaction, "counters");
				countersGroups = transaction.State.GetTree(transaction, "groups");
				etagsToCounters = transaction.State.GetTree(transaction, "etags->counters");
				countersToEtag = transaction.State.GetTree(transaction, "counters->etags");
				groupAndCounterName = transaction.State.GetTree(transaction, "groupAndCounterName");
				metadata = transaction.State.GetTree(transaction, "$metadata");
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

			public CounterChangeAction Store(string group, string counterName, long delta)
			{
				//TODO: decide what to do when delta=0 and the counter already exists

				//var mergedGroupAndName = MergeGroupAndName(groupName, counterName);
				var sign = delta >= 0 ? ValueSign.Positive : ValueSign.Negative;
				//var fullCounterName = string.Concat(group, Separator, counterName, Separator, parent.ServerId, Separator, sign);
				var doesCounterExist = Store(group, counterName, parent.ServerId, sign, counterKey =>
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
				//var sign = counterValue.IsPositive ? ValueSign.Positive : ValueSign.Negative;
				//TODO: verify counter name stracture
				var sign = counterValue.IsPositive() ? ValueSign.Positive : ValueSign.Negative;
				var doesCounterExist = Store(counterValue.Group(), counterValue.CounterName(), counterValue.ServerId(), sign, counterKey =>
				{
					var sliceWriter = new SliceWriter(sizeof(long));
					sliceWriter.WriteLittleEndian(counterValue.Value);
					counters.Add(counterKey, sliceWriter.CreateSlice());
				});

				if (doesCounterExist)
					return counterValue.Value >= 0 ? CounterChangeAction.Increment : CounterChangeAction.Decrement;

				return CounterChangeAction.Add;
			}

			// full counter name: foo/bar/server-id/+
			private bool Store(string group, string counterName, Guid serverId, char sign, Action<Slice> storeAction)
			{
				var groupSize = Encoding.UTF8.GetByteCount(group);
				var counterNameSize = Encoding.UTF8.GetByteCount(counterName);
				var fullCounterNameSize = groupSize + 1 + counterNameSize + 1 + 36 + 1 + sizeof(char);
				//var requiredBufferSize = fullCounterNameSize + 36 + 2;
				//Debug.Assert(fullCounterNameSize < UInt16.MaxValue);

				//EnsureBufferSize(fullCounterNameSize);
				var sliceWriter = new SliceWriter(fullCounterNameSize);
				sliceWriter.WriteString(group);

				sliceWriter.WriteString(Constants.Counter.Separator);
				sliceWriter.WriteString(counterName);
				sliceWriter.WriteString(Constants.Counter.Separator);
				var groupWithCounterName = sliceWriter.CreateSlice();
				var doesCounterExist = DoesCounterExist(groupWithCounterName);
				if (doesCounterExist == false)
				{
					//it's a new counter in the group
					var groupKey = sliceWriter.CreateSlice(groupSize);
					countersGroups.Increment(groupKey, 1);
				} 
					
				groupAndCounterName.Add(groupWithCounterName, new byte[0]);

				sliceWriter.WriteString(serverId.ToString());
				sliceWriter.WriteString(Constants.Counter.Separator);
				sliceWriter.WriteLittleEndian(sign);

				//save counter full name and its value into the counters tree
				var counterKey = sliceWriter.CreateSlice();
				storeAction(counterKey);

				var readResult = countersToEtag.Read(counterKey);
				if (readResult != null) // remove old etag entry
				{
					readResult.Reader.Read(etagBuffer, 0, sizeof(long));
					var oldEtagSlice = new Slice(etagBuffer);
					etagsToCounters.Delete(oldEtagSlice);
				}

				parent.lastEtag++;
				EndianBitConverter.Little.CopyBytes(parent.lastEtag, etagBuffer, 0);
				var newEtagSlice = new Slice(etagBuffer);
				etagsToCounters.Add(newEtagSlice, counterKey);
				countersToEtag.Add(counterKey, newEtagSlice);

				return doesCounterExist;
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

			private void EnsureBufferSize(int requiredBufferSize)
			{
				if (fullCounterNameBuffer.Length < requiredBufferSize)
					fullCounterNameBuffer = new byte[Utils.NearestPowerOfTwo(requiredBufferSize)];
			}

			public void RecordLastEtagFor(Guid serverId, long lastEtag)
			{
				//TODO: consider changing that
				serversLastEtag.Add(serverId.ToString(), EndianBitConverter.Little.GetBytes(lastEtag));
			}

			public void UpdateReplications(CountersReplicationDocument newReplicationDocument)
			{
				using (var memoryStream = new MemoryStream())
				using (var streamWriter = new StreamWriter(memoryStream))
				using (var jsonTextWriter = new JsonTextWriter(streamWriter))
				{
					new JsonSerializer().Serialize(jsonTextWriter, newReplicationDocument);
					streamWriter.Flush();
					memoryStream.Position = 0;
					metadata.Add("replication", memoryStream);
				}

				parent.replicationTask.SignalCounterUpdate();
			}

			private bool DoesCounterExist(Slice name)
			{
				using (var it = counters.Iterate())
				{
					it.RequiredPrefix = name;
					return it.Seek(name);
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

		private static long CalculateOverallTotal(Counter counterValuesByPrefix)
		{
			return counterValuesByPrefix.CounterValues.Sum(x => x.IsPositive() ? x.Value : -x.Value);
		}

		private static Slice MergeGroupAndName(string group, string counterName)
		{
			var groupSize = Encoding.UTF8.GetByteCount(group);
			var counterNameSize = Encoding.UTF8.GetByteCount(counterName);
			var sliceWriter = new SliceWriter(groupSize + counterNameSize + 3);
			sliceWriter.WriteString(group);
			sliceWriter.WriteString(Constants.Counter.Separator);
			sliceWriter.WriteString(counterName);
			sliceWriter.WriteString(Constants.Counter.Separator);
			return sliceWriter.CreateSlice();
		}

		public const string GroupAndNameSeparator = "/";

		public class ServerEtag
		{
			public string ServerId { get; set; }
			public long Etag { get; set; }
		}

		string IResourceStore.Name
		{
			get { return Name; }
		}

		public StorageEnvironment CounterStorageEnvironment
		{
			get { return storageEnvironment; }
		}
	}
}