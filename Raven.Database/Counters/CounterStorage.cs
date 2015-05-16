using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Newtonsoft.Json;
using Raven.Abstractions;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Client.Counters;
using Raven.Database.Config;
using Raven.Database.Counters.Controllers;
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
using JsonSerializer = Newtonsoft.Json.JsonSerializer;

namespace Raven.Database.Counters
{
	public class CounterStorage : IDisposable, IResourceStore
	{
		private readonly ILog Log = LogManager.GetCurrentClassLogger();
		public string CounterStorageUrl { get; private set; }
		private readonly StorageEnvironment storageEnvironment;
		public readonly CountersReplicationTask replicationTask;

		public DateTime LastWrite { get; private set; }

		private long lastEtag;

		public string ServerId { get; set; }

		public event Action CounterUpdated = () => { };

		public event Action ReplicationUpdated;

		public string Name { get; private set; }
		public int ReplicationTimeoutInMs { get; private set; }

		public string ResourceName { get; private set; }
		private readonly CountersMetricsManager metricsCounters;

		private readonly TransportState transportState;

		public CounterStorage(string serverUrl, string storageName, InMemoryRavenConfiguration configuration, TransportState recievedTransportState = null)
		{
			CounterStorageUrl = String.Format("{0}counters/{1}", serverUrl, storageName);
			Name = storageName;
			ResourceName = string.Concat(Constants.Counter.UrlPrefix, "/", storageName);

			var options = configuration.RunInMemory ? StorageEnvironmentOptions.CreateMemoryOnly()
				: CreateStorageOptionsFromConfiguration(configuration.CountersDataDirectory, configuration.Settings);

			storageEnvironment = new StorageEnvironment(options);
			replicationTask = new CountersReplicationTask(this);

			replicationTask.ReplicationUpdate += OnReplicationUpdated;

			ReplicationTimeoutInMs = configuration.Replication.ReplicationRequestTimeoutInMilliseconds;

			metricsCounters = new CountersMetricsManager();
			transportState = recievedTransportState ?? new TransportState();
			Configuration = configuration;
			ExtensionsState = new AtomicDictionary<object>();
			Initialize();
		}

		protected virtual void OnReplicationUpdated()
		{
			var replicationUpdated = ReplicationUpdated;
			if (replicationUpdated != null) replicationUpdated();
		}

		private void Initialize()
		{
			using (var tx = storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
			{
				storageEnvironment.CreateTree(tx, "servers->lastEtag");
				storageEnvironment.CreateTree(tx, "counters");
				storageEnvironment.CreateTree(tx, "countersGroups");
				storageEnvironment.CreateTree(tx, "counters->etags");

				var etags = storageEnvironment.CreateTree(tx, "etags->counters");
				var metadata = storageEnvironment.CreateTree(tx, "$metadata");
				var id = metadata.Read("id");

				if (id == null) // new counter db
				{
					var newGuid = Guid.NewGuid();
					ServerId = newGuid.ToString();
					var serverIdBytes = newGuid.ToByteArray();

					metadata.Add("id", serverIdBytes);

					tx.Commit();
				}
				else // existing counter db
				{
					int used;
					ServerId = new Guid(id.Reader.ReadBytes(16, out used)).ToString();

					using (var it = etags.Iterate())
					{
						if (it.Seek(Slice.AfterAllKeys))
						{
							lastEtag = it.CurrentKey.CreateReader().ReadBigEndianInt64();
						}
					}
				}

				replicationTask.StartReplication();
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
		public AtomicDictionary<object> ExtensionsState { get; private set; }

		public InMemoryRavenConfiguration Configuration { get; private set; }

		public CounterStorageStats CreateStats()
		{
			using (var reader = CreateReader())
			{
				var stats = new CounterStorageStats
				{
					CounterStorageName = Name,
					Url = CounterStorageUrl,
					CountersCount = reader.GetCountersCount(),
					LastCounterEtag = lastEtag,
					TasksCount = replicationTask.GetActiveTasksCount(),
					CounterStorageSize = SizeHelper.Humane(storageEnvironment.Stats().UsedDataFileSizeInBytes),
					GroupsCount = reader.GetGroupsCount(),
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
			return new Reader(storageEnvironment.NewTransaction(TransactionFlags.Read));
		}

		[CLSCompliant(false)]
		public Writer CreateWriter()
		{
			LastWrite = SystemTime.UtcNow;
			return new Writer(this, storageEnvironment.NewTransaction(TransactionFlags.ReadWrite));
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
			private readonly Tree serversLastEtag, counters, countersToEtags, countersGroups, etagsToCounters, metadata;
			private readonly byte[] serverIdBytes = new byte[16];

			[CLSCompliant(false)]
			public Reader(Transaction transaction)
			{
				this.transaction = transaction;
				serversLastEtag = transaction.State.GetTree(transaction, "servers->lastEtag");
				counters = transaction.State.GetTree(transaction, "counters");
				countersGroups = transaction.State.GetTree(transaction, "countersGroups");
				countersToEtags = transaction.State.GetTree(transaction, "counters->etags");
				etagsToCounters = transaction.State.GetTree(transaction, "etags->counters");
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

			public bool CounterExists(string groupName, string counterName)
			{
				var name = MergeGroupAndName(groupName, counterName);
				using (var it = counters.Iterate())
				{
					it.RequiredPrefix = name;
					return it.Seek(name);
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

			public CounterValue GetCounterValue(string fullCounterName)
			{
				var readResult = counters.Read(fullCounterName);
				if (readResult == null)
					return null;

				return new CounterValue
				{
					Name = fullCounterName,
					Value = readResult.Reader.ReadLittleEndianInt64(),
				};
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
				return GetCounterValuesByPrefix(MergeGroupAndName(groupName, counterName));
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
						{
							Name = it.CurrentKey.ToString(),
							Value = it.CreateReaderForCurrent().ReadLittleEndianInt64(),
						});
					} while (it.MoveNext());
					return result;
				}
			}

			public IEnumerable<ReplicationCounter> GetCountersSinceEtag(long etag)
			{
				using (var it = etagsToCounters.Iterate())
				{
					var buffer = new byte[sizeof(long)];
					EndianBitConverter.Big.CopyBytes(etag, buffer, 0);
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
						var counterEtag = etagResult == null ? 0 : etagResult.Reader.ReadBigEndianInt64();

						var counterValue = GetCounterValue(fullCounterName);
						yield return new ReplicationCounter
						{
							FullCounterName = fullCounterName,
							Etag = counterEtag,
							CounterValue = counterValue
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
							Etag = EndianBitConverter.Big.ToInt64(buffer, 0),
						};

					} while (it.MoveNext());
				}
			}

			public long GetLastEtagFor(string serverId)
			{
				var result = serversLastEtag.Read(serverId);
				return result != null ? result.Reader.ReadLittleEndianInt64() : 0;
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
			private readonly Tree counters, serversLastEtag, etagsToCounters, countersToEtag, countersGroups, metadata;
			//private readonly byte[] tempThrowawayBuffer = new byte[sizeof(long)];
			private byte[] fullCounterNameBuffer = new byte[0];
			private readonly byte[] counterValueBuffer = new byte[sizeof(long)];
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
				countersGroups = transaction.State.GetTree(transaction, "countersGroups");
				etagsToCounters = transaction.State.GetTree(transaction, "etags->counters");
				countersToEtag = transaction.State.GetTree(transaction, "counters->etags");
				metadata = transaction.State.GetTree(transaction, "$metadata");
			}

			public CounterValue GetCounterValue(string fullCounterName)
			{
				return reader.GetCounterValue(fullCounterName);
			}

			private Counter GetCounterValuesByPrefix(string groupName, string counterName)
			{
				return reader.GetCounterValuesByPrefix(groupName, counterName);
			}

			public long GetLastEtagFor(string serverId)
			{
				return reader.GetLastEtagFor(serverId);
			}

			public void Store(string groupName, string counterName, long delta)
			{
				//var mergedGroupAndName = MergeGroupAndName(groupName, counterName);
				var sign = delta >= 0 ? ValueSign.Positive : ValueSign.Negative;
				var fullCounterName = string.Concat(groupName, Separator, counterName, Separator, parent.ServerId, Separator, sign);
				Store(fullCounterName, counterKey =>
				{
					if (sign == ValueSign.Negative)
						delta = -delta;
					counters.Increment(counterKey, delta);
				});
			}

			public void Store(string fullCounterName, CounterValue counterValue)
			{
				//var sign = counterValue.IsPositive ? ValueSign.Positive : ValueSign.Negative;
				//TODO: verify counter name stracture
				Store(fullCounterName, counterKey =>
				{
					EndianBitConverter.Little.CopyBytes(counterValue.Value, counterValueBuffer, 0);
					counters.Add(counterKey, counterValueBuffer);
				});
			}

			// full counter name: foo/bar/guid/+
			private void Store(string fullCounterName, Action<Slice> storeAction)
			{
				var fullCounterNameSize = Encoding.UTF8.GetByteCount(fullCounterName);
				//var requiredBufferSize = fullCounterNameSize + 36 + 2;
				Debug.Assert(fullCounterNameSize < UInt16.MaxValue);

				EnsureBufferSize(fullCounterNameSize);
				var sliceWriter = new SliceWriter(fullCounterNameBuffer);
				sliceWriter.WriteString(fullCounterName);
				/*sliceWriter.WriteString(counterName);
				sliceWriter.WriteString(serverId);
				sliceWriter.WriteString(Separator);
				sliceWriter.WriteBigEndian(sign);*/

				var endOfGroupNameIndex = fullCounterName.IndexOf(Separator, StringComparison.InvariantCultureIgnoreCase);
				if (endOfGroupNameIndex == -1)
					throw new InvalidOperationException("Could not find group name in counter, no separator");

				var counterKey = sliceWriter.CreateSlice();
				if (DoesCounterExist(counterKey) == false) //it's a new counter
				{
					Slice groupKey = fullCounterName.Substring(0, endOfGroupNameIndex);
					countersGroups.Increment(groupKey, 1);
				}

				//save counter full name and its value into the counters tree
				storeAction(counterKey);

				var readResult = countersToEtag.Read(counterKey);
				if (readResult != null) // remove old etag entry
				{
					readResult.Reader.Read(etagBuffer, 0, sizeof(long));
					var oldEtagSlice = new Slice(etagBuffer);
					etagsToCounters.Delete(oldEtagSlice);
				}

				parent.lastEtag++;
				EndianBitConverter.Big.CopyBytes(parent.lastEtag, etagBuffer, 0);
				var newEtagSlice = new Slice(etagBuffer);
				etagsToCounters.Add(newEtagSlice, counterKey);
				countersToEtag.Add(counterKey, newEtagSlice);
			}

			public bool Reset(string groupName, string counterName)
			{
				var counterValuesByPrefix = GetCounterValuesByPrefix(groupName, counterName);
				if (counterValuesByPrefix == null)
					return false;

				var difference = CalculateOverallTotal(counterValuesByPrefix);
				if (difference != 0)
				{
					difference = -difference;
					Store(groupName, counterName, difference);
					parent.MetricsCounters.Resets.Mark();
					return true;
				}
				return false;
			}

			private void EnsureBufferSize(int requiredBufferSize)
			{
				if (fullCounterNameBuffer.Length < requiredBufferSize)
					fullCounterNameBuffer = new byte[Utils.NearestPowerOfTwo(requiredBufferSize)];
			}

			public void RecordLastEtagFor(string serverId, long lastEtag)
			{
				//TODO: remove server name
				serversLastEtag.Add(serverId, EndianBitConverter.Big.GetBytes(lastEtag));
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
			return counterValuesByPrefix.CounterValues.Sum(x => x.IsPositive ? x.Value : -x.Value);
		}

		private static string MergeGroupAndName(string groupName, string counterName)
		{
			return string.Concat(groupName, Separator, counterName, Separator);
		}

		private const string Separator = "/";

		public class ServerEtag
		{
			public string ServerId { get; set; }
			public long Etag { get; set; }
		}

		string IResourceStore.Name
		{
			get { return Name; }
		}

	}
}