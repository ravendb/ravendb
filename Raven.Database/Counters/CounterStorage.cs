using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Newtonsoft.Json;
using Raven.Abstractions;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Config;
using Raven.Database.Counters.Controllers;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Connections;
using Raven.Database.Util;
using Voron;
using Voron.Debugging;
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
		public readonly RavenCounterReplication ReplicationTask;

		public DateTime LastWrite { get; private set; }

		private long lastEtag;

		private string ServerId { get; set; }

		public event Action CounterUpdated = () => { };

		public int ReplicationTimeoutInMs { get; private set; }

		public readonly string Name;

		private readonly CountersMetricsManager metricsCounters;

		private readonly TransportState transportState;

		public CounterStorage(string serverUrl, string storageName, InMemoryRavenConfiguration configuration, TransportState recievedTransportState = null)
		{
			CounterStorageUrl = String.Format("{0}counters/{1}", serverUrl, storageName);
			Name = storageName;

			var options = configuration.RunInMemory ? StorageEnvironmentOptions.CreateMemoryOnly()
				: CreateStorageOptionsFromConfiguration(configuration.CountersDataDirectory, configuration.Settings);

			storageEnvironment = new StorageEnvironment(options);
			ReplicationTask = new RavenCounterReplication(this);

			ReplicationTimeoutInMs = configuration.Replication.ReplicationRequestTimeoutInMilliseconds;

			metricsCounters = new CountersMetricsManager();
			transportState = recievedTransportState ?? new TransportState();
			Configuration = configuration;
			ExtensionsState = new AtomicDictionary<object>();
			Initialize();
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
					ServerId = Guid.NewGuid().ToString();
					var serverIdBytes = Guid.NewGuid().ToByteArray();

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

				ReplicationTask.StartReplication();
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
					TasksCount = ReplicationTask.GetActiveTasksCount(),
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
			return new Writer(this, storageEnvironment);
		}

		private void Notify()
		{
			CounterUpdated();
		}

		public void Dispose()
		{
			var exceptionAggregator = new ExceptionAggregator(Log, "Could not properly dispose of CounterStorage: " + Name);

			if (ReplicationTask != null)
				exceptionAggregator.Execute(ReplicationTask.Dispose);

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
			private readonly byte[] signBytes = new byte[sizeof(char)];

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
							NumOfCounters = it.CreateReaderForCurrent().ReadBigEndianInt64()
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
				return result != null ? result.Reader.ReadBigEndianInt64() : 0;
			}

			public CounterStorageReplicationDocument GetReplicationData()
			{
				var readResult = metadata.Read("replication");
				if (readResult == null) 
					return null;

				var stream = readResult.Reader.AsStream();
				stream.Position = 0;
				using (var streamReader = new StreamReader(stream))
				using (var jsonTextReader = new JsonTextReader(streamReader))
				{
					return new JsonSerializer().Deserialize<CounterStorageReplicationDocument>(jsonTextReader);
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
			private readonly WriteBatch writeBatch;
			private readonly SnapshotReader snapshotReader;
			private readonly Tree serversLastEtag, counters, etagsToCounters, countersToEtag, countersGroups, metadata;
			private readonly byte[] tempThrowawayBuffer;
			private readonly byte[] etagBuffer = new byte[sizeof(long)];
			private Reader reader;
			public Writer(CounterStorage parent, StorageEnvironment storageEnvironment)
			{
				this.parent = parent;
				transaction = storageEnvironment.NewTransaction(TransactionFlags.Read);
				reader = new Reader(transaction);
				writeBatch = new WriteBatch();
				snapshotReader = new SnapshotReader(transaction);
				serversLastEtag = transaction.State.GetTree(transaction, "servers->lastEtag");
				counters = transaction.State.GetTree(transaction, "counters");
				countersGroups = transaction.State.GetTree(transaction, "countersGroups");
				etagsToCounters = transaction.State.GetTree(transaction, "etags->counters");
				countersToEtag = transaction.State.GetTree(transaction, "counters->etags");
				metadata = transaction.State.GetTree(transaction, "$metadata");

				tempThrowawayBuffer = new byte[sizeof(long)];
			}

			public CounterValue GetCounterValue(string fullCounterName)
			{
				return reader.GetCounterValue(fullCounterName);
			}

			private Counter GetCounterValuesByPrefix(string groupName, string counterName)
			{
				return reader.GetCounterValuesByPrefix(groupName, counterName);
			}

			public long GetLastEtagFor(string serverName)
			{
				return reader.GetLastEtagFor(serverName);
			}

			public void Store(string groupName, string counterName, long delta)
			{
				var fullCounterName = MergeGroupAndName(groupName, counterName);
				var sign = delta >= 0 ? ValueSign.Positive : ValueSign.Negative;
				Store(parent.ServerId, fullCounterName, sign, counterKey =>
				{
					if (delta < 0)
						delta = -delta;
					writeBatch.Increment(counterKey, delta, counters.Name);
				});
			}

			public void Store(string fullCounterName, CounterValue counterValue)
			{
				var sign = counterValue.IsPositive ? ValueSign.Positive : ValueSign.Negative;
				var serverId = counterValue.ServerId.ToString();
				Store(serverId, fullCounterName, sign, counterKey =>
				{
					EndianBitConverter.Big.CopyBytes(counterValue.Value, tempThrowawayBuffer, 0);
					var valueSlice = new Slice(tempThrowawayBuffer);
					writeBatch.Add(counterKey, valueSlice, counters.Name, shouldIgnoreConcurrencyExceptions: true);
				});
			}

			// counter name: foo/bar/
			private void Store(string serverId, string counterName, string sign, Action<Slice> storeAction)
			{
				var fullCounterNameSize = Encoding.UTF8.GetByteCount(counterName);
				var requiredBufferSize = fullCounterNameSize + 36 + 2;
				Debug.Assert(requiredBufferSize < UInt16.MaxValue);

				var sliceWriter = new SliceWriter();
				sliceWriter.WriteString(counterName);
				sliceWriter.WriteString(serverId);
				sliceWriter.WriteString("/");
				sliceWriter.WriteString(sign);

				var endOfGroupNameIndex = counterName.IndexOf('/');
				if (endOfGroupNameIndex == -1)
					throw new InvalidOperationException("Could not find group name in counter, no separator");

				var counterKey = sliceWriter.CreateSlice(requiredBufferSize);
				var readResult = snapshotReader.Read(counters.Name, counterKey, writeBatch);
				if (readResult == null && DoesCounterExist(counterName) == false) //it's a new counter
				{
					Slice groupKey = counterName.Substring(0, endOfGroupNameIndex);
					writeBatch.Add(groupKey, new MemoryStream(), countersGroups.Name);
				}

				//save counter full name and its value into the counters tree
				storeAction(counterKey);

				readResult = snapshotReader.Read(countersToEtag.Name, counterKey, writeBatch);

				if (readResult != null) // remove old etag entry
				{
					readResult.Reader.Read(etagBuffer, 0, sizeof(long));
					var oldEtagSlice = new Slice(etagBuffer);
					writeBatch.Delete(oldEtagSlice, etagsToCounters.Name);
				}

				var lastEtag = Interlocked.Increment(ref parent.lastEtag);
				EndianBitConverter.Big.CopyBytes(lastEtag, etagBuffer, 0);
				var newEtagSlice = new Slice(etagBuffer);
				writeBatch.Add(newEtagSlice, counterKey, etagsToCounters.Name, shouldIgnoreConcurrencyExceptions: true);
				writeBatch.Add(counterKey, newEtagSlice, etagsToCounters.Name, shouldIgnoreConcurrencyExceptions: true);
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

			public void RecordLastEtagFor(string serverId, string serverName, long lastEtag)
			{
				serversLastEtag.Add(serverId, EndianBitConverter.Big.GetBytes(lastEtag));
			}

			public void UpdateReplications(CounterStorageReplicationDocument newReplicationDocument)
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

				parent.ReplicationTask.SignalCounterUpdate();
			}

			private bool DoesCounterExist(string name)
			{
				using (var it = counters.Iterate())
				{
					it.RequiredPrefix = name;
					return it.Seek(name);
				}
			}

			public void Commit(bool notifyParent = true)
			{
				parent.storageEnvironment.Writer.Write(writeBatch);
				if (notifyParent)
				{
					parent.Notify();
				}
			}

			public void Dispose()
			{
				writeBatch.Dispose();
				//snapshotReader.Dispose();
				parent.LastWrite = SystemTime.UtcNow;
				if (transaction != null)
					transaction.Dispose();
			}
		}

		private static string MergeGroupAndName(string groupName, string counterName)
		{
			return String.Concat(groupName, "/", counterName, "/");
		}

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