using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Mono.CSharp;
using Raven.Abstractions;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Counters.Notifications;
using Raven.Abstractions.Extensions;
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
		private readonly Guid tombstoneId = Guid.Empty;
		private readonly int sizeOfGuid;
		private Timer purgeTombstonesTimer;
		private TimeSpan tombstoneRetentionTime;

		private long lastEtag;
		private long lastCounterId;
		public event Action CounterUpdated = () => { };

		public string CounterStorageUrl { get; private set; }

		public DateTime LastWrite { get; private set; }

		public Guid ServerId { get; private set; }

		public string Name { get; private set; }

		public string ResourceName { get; private set; }

		public int ReplicationTimeoutInMs { get; private set; }

		public unsafe CounterStorage(string serverUrl, string storageName, InMemoryRavenConfiguration configuration, TransportState receivedTransportState = null)
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
			tombstoneRetentionTime = configuration.Counter.TombstoneRetentionTime;
			metricsCounters = new CountersMetricsManager();
			Configuration = configuration;
			ExtensionsState = new AtomicDictionary<object>();
			jsonSerializer = new JsonSerializer();
			sizeOfGuid = sizeof(Guid); //TODO: unsafe?

			Initialize();
			purgeTombstonesTimer = new Timer(BackgroundActionsCallback, null, TimeSpan.Zero, TimeSpan.FromHours(1));
		}

		private void Initialize()
		{
			using (var tx = CounterStorageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
			{
				storageEnvironment.CreateTree(tx, TreeNames.ServersLastEtag);
				storageEnvironment.CreateTree(tx, TreeNames.Counters);
				storageEnvironment.CreateTree(tx, TreeNames.Tombstones);
				storageEnvironment.CreateTree(tx, TreeNames.GroupToCounters);
				storageEnvironment.CreateTree(tx, TreeNames.CounterIdWithNameToGroup);
				storageEnvironment.CreateTree(tx, TreeNames.CountersToEtag);
				
				var etags = CounterStorageEnvironment.CreateTree(tx, TreeNames.EtagsToCounters);
				var metadata = CounterStorageEnvironment.CreateTree(tx, TreeNames.Metadata);
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
					

					using (var it = etags.Iterate())
					{
						if (it.Seek(Slice.AfterAllKeys))
						{
							lastEtag = it.CurrentKey.CreateReader().ReadBigEndianInt64();
						}
					}
				}

				if (lastCounterIdRead == null)
				{
					var buffer = new byte[sizeof (long)];
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
			while (true)
			{
				using (var writer = CreateWriter())
				{
					if (writer.PurgeOutdatedTombstones() == false)
						break;

					writer.Commit();
				}
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

			if (purgeTombstonesTimer != null)
				exceptionAggregator.Execute(purgeTombstonesTimer.Dispose);
			purgeTombstonesTimer = null;

			exceptionAggregator.ThrowIfNeeded();
		}

		[CLSCompliant(false)]
		public class Reader : IDisposable
		{
			private readonly Transaction transaction;
			private readonly Tree counters, tombstonesByDate, groupToCounters, counterIdWithNameToGroup, etagsToCounters, countersToEtag, serversLastEtag, metadata;
			private readonly CounterStorage parent;

			[CLSCompliant(false)]
			public Reader(CounterStorage parent, Transaction transaction)
			{
				this.transaction = transaction;
				this.parent = parent;
				counters = transaction.State.GetTree(transaction, TreeNames.Counters);
				tombstonesByDate = transaction.State.GetTree(transaction, TreeNames.Tombstones);
				groupToCounters = transaction.State.GetTree(transaction, TreeNames.GroupToCounters);
				counterIdWithNameToGroup = transaction.State.GetTree(transaction, TreeNames.CounterIdWithNameToGroup);
				countersToEtag = transaction.State.GetTree(transaction, TreeNames.CountersToEtag);
				etagsToCounters = transaction.State.GetTree(transaction, TreeNames.EtagsToCounters);
				serversLastEtag = transaction.State.GetTree(transaction, TreeNames.ServersLastEtag);
				metadata = transaction.State.GetTree(transaction, TreeNames.Metadata);
			}

			public long GetCountersCount()
			{
				long countersCount = 0;
				using (var it = groupToCounters.Iterate())
				{
					if (it.Seek(Slice.BeforeAllKeys) == false)
						return countersCount;

					var counterIdBytes = new byte[sizeof(long)];
					var tombstoneBuffer = new byte[sizeof(long) + parent.sizeOfGuid + sizeof(char)];
					do
					{
						countersCount += GetCountersCountInGroup(it.CurrentKey, counterIdBytes, tombstoneBuffer);
					} while (it.MoveNext());
				};
				return countersCount;
			}

			private long GetCountersCountInGroup(Slice groupSlice, byte[] counterIdBytes, byte[] tombstoneBuffer)
			{
				var count = 0;
				using (var it = groupToCounters.MultiRead(groupSlice))
				{
					if (it.Seek(Slice.BeforeAllKeys) == false)
						return 0;

					do
					{
						var valueReader = it.CurrentKey.CreateReader();
						valueReader.Skip(it.CurrentKey.Size - sizeof(long));
						valueReader.Read(counterIdBytes, 0, sizeof(long));
						var tombstoneSlice = GetTombstoneSlice(tombstoneBuffer, counterIdBytes, parent.tombstoneId);
						var tombstone = counters.Read(tombstoneSlice);
						if (tombstone == null)
							count++;
					} while (it.MoveNext());
				}
				return count;
			}

			public long GetGroupsCount()
			{
				long groupsCount = 0;
				using (var it = groupToCounters.Iterate())
				{
					if (it.Seek(Slice.BeforeAllKeys) == false)
						return groupsCount;

					var counterIdBytes = new byte[sizeof(long)];
					var tombstoneBuffer = new byte[sizeof(long) + parent.sizeOfGuid + sizeof(char)];
					do
					{
						if (GetCountersCountInGroup(it.CurrentKey, counterIdBytes, tombstoneBuffer) > 0)
							groupsCount++;
					} while (it.MoveNext());
				};
				return groupsCount;
			}

			public bool CounterExists(string group, string counterName)
			{
				throw new Exception();
				/*var slice = MergeGroupAndName(group, counterName);
				using (var it = groupAndCounterName.Iterate())
				{
					it.RequiredPrefix = slice;
					return it.Seek(slice);
				}*/
			}

			public IEnumerable<string> GetCountersByPrefixes(string groupsPrefix, int skip = 0, int take = int.MaxValue)
			{
				Debug.Assert(take > 0);
				Debug.Assert(skip >= 0);
				throw new Exception();
				/*using (var it = groupAndCounterName.Iterate())
				{
					it.RequiredPrefix = groupsPrefix;
					if (it.Seek(it.RequiredPrefix) == false || it.Skip(skip) == false)
						yield break;

					do
					{
						yield return it.CurrentKey.ToString();
					} while (it.MoveNext() && --take > 0);
				}*/
			}

			private class CounterDetails
			{
				public byte[] IdBytes { get; set; }
				public string Name { get; set; }
				public string Group { get; set; }
			}


			public bool DoesCounterExist(string groupName, string counterName)
			{
				using (var it = groupToCounters.MultiRead(groupName))
				{
					it.RequiredPrefix = counterName;
					if (it.Seek(it.RequiredPrefix) == false || it.CurrentKey.Size != it.RequiredPrefix.Size + sizeof(long))
						return false;

					return true;
				}
			}

			private IEnumerable<CounterDetails> GetCountersDetails(string groupName, int skip, int take)
			{
				var nameBuffer = new byte[0];
				var counterIdBytes = new byte[sizeof(long)];
				var tombstoneBuffer = new byte[sizeof(long) + parent.sizeOfGuid + sizeof(char)];
				using (var it = groupToCounters.Iterate())
				{
					it.RequiredPrefix = groupName;
					if (it.Seek(it.RequiredPrefix) == false)
						yield break;

					do
					{
						var countersInGroup = GetCountersCountInGroup(it.CurrentKey, counterIdBytes, tombstoneBuffer);
						if (skip - countersInGroup <= 0)
							break;
						skip -= (int)countersInGroup; //TODO: is there a better way?
					} while (it.MoveNext());

					do
					{
						using (var iterator = groupToCounters.MultiRead(it.CurrentKey))
						{
							iterator.Skip(skip);
							if (iterator.Seek(Slice.BeforeAllKeys) == false)
								yield break;

							do
							{
								var valueReader = iterator.CurrentKey.CreateReader();
								var requiredBufferSize = iterator.CurrentKey.Size - sizeof(long);
								valueReader.Skip(requiredBufferSize);
								valueReader.Read(counterIdBytes, 0, sizeof(long));

								var tombstoneSlice = GetTombstoneSlice(tombstoneBuffer, counterIdBytes, parent.tombstoneId);
								var tombstone = counters.Read(tombstoneSlice);
								if (tombstone != null)
									continue;

								var counterDetails = new CounterDetails
								{
									Group = groupName.Equals(string.Empty) ? it.CurrentKey.ToString() : groupName
								};

								EnsureBufferSize(ref nameBuffer, requiredBufferSize);
								valueReader.Reset();
								valueReader.Read(nameBuffer, 0, requiredBufferSize);
								counterDetails.Name = Encoding.UTF8.GetString(nameBuffer, 0, nameBuffer.Length);
								counterDetails.IdBytes = counterIdBytes;
								yield return counterDetails;
							} while (iterator.MoveNext() && --take > 0);
						}
					} while (it.MoveNext() && --take > 0);
				}
			}


			public List<CounterSummary> GetCountersSummary(string groupName, int skip = 0, int take = int.MaxValue)
			{
				var summaryList = new List<CounterSummary>();
				var countersDetails = GetCountersDetails(groupName, skip, take);
				foreach (var counterDetails in countersDetails)
				{
					var counterSummary = new CounterSummary
					{
						Group = counterDetails.Group,
						CounterName = counterDetails.Name,
						Total = CalculateCounterTotal(counterDetails.IdBytes, new byte[parent.sizeOfGuid])
					};
					summaryList.Add(counterSummary);
				}
				return summaryList;
			}

			/*public long CalculateCounterTotal(string groupName, string counterName)
			{
				var readResult = groupToCounters.Read(groupName);
				if (readResult == null)
					return 0; //TODO: throw

				using (var it = groupToCounters.MultiRead(counterName))
				{
					it.RequiredPrefix = counterName;
					if (it.Seek(it.RequiredPrefix) == false || it.CurrentKey.Size != it.RequiredPrefix.Size + sizeof(long))
						return 0; //TODO: throw

					var valueReader = it.CurrentKey.CreateReader();
					valueReader.Skip(it.CurrentKey.Size - sizeof(long));
					var counterIdBytes = new byte[sizeof (long)];
					valueReader.Read(counterIdBytes, 0, sizeof (long));
					return CalculateCounterTotal(counterIdBytes);
				}
			}*/

			private long CalculateCounterTotal(byte[] counterIdBytes, byte[] serverIdBytes)
			{
				using (var it = counters.Iterate())
				{
					var slice = new Slice(counterIdBytes);
					it.RequiredPrefix = slice;
					if (it.Seek(it.RequiredPrefix) == false)
						return 0; //TODO: throw exception

					long total = 0;
					do
					{
						var reader = it.CurrentKey.CreateReader();
						reader.Skip(sizeof(long));
						reader.Read(serverIdBytes, 0, parent.sizeOfGuid);
						var serverId = new Guid(serverIdBytes);
						if (serverId.Equals(parent.tombstoneId))
							continue;

						var lastByte = it.CurrentKey[it.CurrentKey.Size - 1];
						var sign = Convert.ToChar(lastByte);
						var value = it.CreateReaderForCurrent().ReadLittleEndianInt64();
						if (sign == ValueSign.Positive)
							total += value;
						else
							total -= value;

						//this means that this used to be a deleted counter
						/*if (isTombestoneId && value == DateTime.MaxValue.Ticks)
							continue;
						if (isTombestoneId)
							throw new Exception(string.Format("Counter was deleted. Group: {0}, Counter Name: {1}", counterSummary.Group, counterSummary.CounterName));*/
					} while (it.MoveNext());

					return total;
				}
			}

			public long GetCounterTotal(string groupName, string counterName)
			{
				using (var it = groupToCounters.MultiRead(groupName))
				{
					it.RequiredPrefix = counterName;
					if (it.Seek(it.RequiredPrefix) == false || it.CurrentKey.Size != it.RequiredPrefix.Size + sizeof (long))
						return -1;//TODO: throw

					var valueReader = it.CurrentKey.CreateReader();
					valueReader.Skip(it.RequiredPrefix.Size);
					var counterIdBytes = new byte[sizeof(long)];
					valueReader.Read(counterIdBytes, 0, sizeof(long));
					//var counterId = valueReader.ReadBigEndianInt64();

					return CalculateCounterTotal(counterIdBytes, new byte[parent.sizeOfGuid]);
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
				using (var it = groupToCounters.Iterate())
				{
					if (it.Seek(Slice.BeforeAllKeys) == false)
						yield break;

					do
					{
						var counterIdBytes = new byte[sizeof(long)];
						var tombstoneBuffer = new byte[sizeof(long) + parent.sizeOfGuid + sizeof(char)];
						yield return new CounterGroup
						{
							Name = it.CurrentKey.ToString(),
							Count = GetCountersCountInGroup(it.CurrentKey, counterIdBytes, tombstoneBuffer)
						};
					} while (it.MoveNext());
				}
			}

			//{counterId}{serverId}{sign}
			internal long GetSingleCounterValue(Slice singleCounterName)
			{
				var readResult = counters.Read(singleCounterName);
				if (readResult == null)
					return -1;

				return readResult.Reader.ReadLittleEndianInt64();
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
						var counterValue = new CounterValue(it.CurrentKey.ToString(), it.CreateReaderForCurrent().ReadLittleEndianInt64());
						if (counterValue.ServerId().Equals(parent.tombstoneId) && counterValue.Value == DateTime.MaxValue.Ticks)
							continue;

						result.CounterValues.Add(counterValue);
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

					var counterIdBuffer = new byte[sizeof(long)];
					var serverIdBuffer = new byte[parent.sizeOfGuid];
					var counterNameBuffer = new byte[0];
					var groupNameBuffer = new byte[0];
					var signBuffer = new byte[sizeof (char)];
					do
					{
						
						//{counterId}{serverId}{sign}
						var valueReader = it.CreateReaderForCurrent();
						valueReader.Read(counterIdBuffer, 0, sizeof(long));
						valueReader.Read(serverIdBuffer, 0, parent.sizeOfGuid);
						var serverId = new Guid(serverIdBuffer);

						/*var lastByte = it.CurrentKey[valueReader.Length - 1];
						var sign = Convert.ToChar(lastByte);*/
						valueReader.Read(signBuffer, 0, sizeof(char));
						var sign = BitConverter.ToChar(signBuffer, 0);
						var singleCounterName = valueReader.AsSlice();
						var value = GetSingleCounterValue(singleCounterName);
						if (sign == ValueSign.Negative)
							value = -value;

						//read counter name and group
						var counterNameAndGroup = GetCounterNameAndGroupByServerId(counterIdBuffer, counterNameBuffer, groupNameBuffer);

						valueReader.Reset();
						var etagResult = countersToEtag.Read(singleCounterName);
						var counterEtag = etagResult == null ? 0 : etagResult.Reader.ReadBigEndianInt64();

						yield return new ReplicationCounter
						{
							GroupName = counterNameAndGroup.GroupName,
							CounterName = counterNameAndGroup.CounterName,
							ServerId = serverId,
							Value = value,
							Etag = counterEtag
						};
					} while (it.MoveNext());
				}
			}

			private class CounterNameAndGroup
			{
				public string CounterName { get; set; }
				public string GroupName { get; set; }
			}

			private CounterNameAndGroup GetCounterNameAndGroupByServerId(byte[] counterIdBuffer, byte[] counterNameBuffer, byte[] groupNameBuffer)
			{
				var counterNameAndGroup = new CounterNameAndGroup();
				using (var it = counterIdWithNameToGroup.Iterate())
				{
					var slice = new Slice(counterIdBuffer);
					it.RequiredPrefix = slice;
					if (it.Seek(it.RequiredPrefix) == false)
						throw new InvalidOperationException("Couldn't find counter id!");

					var counterNameSize = it.CurrentKey.Size - sizeof(long);
					EnsureBufferSize(ref counterNameBuffer, counterNameSize);
					var reader = it.CurrentKey.CreateReader();
					reader.Skip(sizeof(long));
					reader.Read(counterNameBuffer, 0, counterNameSize);
					counterNameAndGroup.GroupName = Encoding.UTF8.GetString(counterNameBuffer, 0, counterNameSize);

					var valueReader = it.CreateReaderForCurrent();
					EnsureBufferSize(ref groupNameBuffer, valueReader.Length);
					valueReader.Read(groupNameBuffer, 0, valueReader.Length);
					counterNameAndGroup.CounterName = Encoding.UTF8.GetString(groupNameBuffer, 0, groupNameBuffer.Length);
				}
				return counterNameAndGroup;
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
							Etag = it.CreateReaderForCurrent().ReadBigEndianInt64()
						};

					} while (it.MoveNext());
				}
			}

			public long GetLastEtagFor(Guid serverId)
			{
				var sliceWriter = new SliceWriter(new byte[parent.sizeOfGuid]);
				sliceWriter.WriteBytes(serverId.ToByteArray());
				var lastEtagBytes = serversLastEtag.Read(sliceWriter.CreateSlice()); 
				return lastEtagBytes != null ? lastEtagBytes.Reader.ReadBigEndianInt64() : 0;
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
			private readonly Tree counters, tombstonesByDate, groupToCounters, counterIdWithNameToGroup, etagsToCounters, countersToEtag, serversLastEtag, metadata;
			private readonly Buffer buffer;

			private class Buffer
			{
				public Buffer(int sizeOfGuid)
				{
					FullCounterName = new byte[sizeof(long) + sizeOfGuid + sizeof(char)];
					FullTombstoneName = new byte[sizeof(long) + sizeOfGuid + sizeof(char)];
				}

				public readonly byte[] FullCounterName;
				public readonly byte[] FullTombstoneName;
				public readonly byte[] Etag = new byte[sizeof(long)];
				public byte[] CounterValue = new byte[sizeof(long)];
				public readonly byte[] CounterId = new byte[sizeof(long)];
				public readonly byte[] TombstoneTicks = new byte[sizeof(long)];
			}

			public Writer(CounterStorage parent, Transaction transaction)
			{
				if (transaction.Flags != TransactionFlags.ReadWrite) //precaution
					throw new InvalidOperationException(string.Format("Counters writer cannot be created with read-only transaction. (tx id = {0})", transaction.Id));

				this.parent = parent;
				this.transaction = transaction;
				reader = new Reader(parent, transaction);
				counters = transaction.State.GetTree(transaction, TreeNames.Counters);
				tombstonesByDate = transaction.State.GetTree(transaction, TreeNames.Tombstones);
				groupToCounters = transaction.State.GetTree(transaction, TreeNames.GroupToCounters);
				counterIdWithNameToGroup = transaction.State.GetTree(transaction, TreeNames.CounterIdWithNameToGroup);
				countersToEtag = transaction.State.GetTree(transaction, TreeNames.CountersToEtag);
				etagsToCounters = transaction.State.GetTree(transaction, TreeNames.EtagsToCounters);
				serversLastEtag = transaction.State.GetTree(transaction, TreeNames.ServersLastEtag);
				metadata = transaction.State.GetTree(transaction, TreeNames.Metadata);
				buffer = new Buffer(parent.sizeOfGuid);
			}

			private bool DoesCounterExist(string groupName, string counterName)
			{
				return reader.DoesCounterExist(groupName, counterName);
			}

			public long GetLastEtagFor(Guid serverId)
			{
				return reader.GetLastEtagFor(serverId);
			}

			public long GetCounterTotal(string groupName, string counterName)
			{
				return reader.GetCounterTotal(groupName, counterName);
			}

			//Local Counters
			public CounterChangeAction Store(string groupName, string counterName, long delta)
			{
				var sign = delta >= 0 ? ValueSign.Positive : ValueSign.Negative;
				var doesCounterExist = Store(groupName, counterName, parent.ServerId, sign, counterKeySlice =>
				{
					if (sign == ValueSign.Negative)
						delta = -delta;
					counters.Increment(counterKeySlice, delta);
				});

				if (doesCounterExist)
					return sign == ValueSign.Positive ? CounterChangeAction.Increment : CounterChangeAction.Decrement;

				return CounterChangeAction.Add;
			}

			//Counters from replication
			public CounterChangeAction Store(string groupName, string counterName, Guid serverId, char sign, long value)
			{
				var doesCounterExist = Store(groupName, counterName, serverId, sign, counterKeySlice =>
				{
					EndianBitConverter.Little.CopyBytes(value, buffer.CounterValue, 0);
					var counterValueSlice = new Slice(buffer.CounterValue);
					counters.Add(counterKeySlice, counterValueSlice);

					if (serverId.Equals(parent.tombstoneId))
					{
						//EndianBitConverter.Little.CopyBytes(value, buffer.CounterValue, 0);
						Array.Reverse(buffer.CounterValue);
						var tombstoneKeySlice = new Slice(buffer.CounterValue);
						tombstonesByDate.MultiAdd(tombstoneKeySlice, counterKeySlice);
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
				var groupSize = Encoding.UTF8.GetByteCount(groupName);
				var sliceWriter = new SliceWriter(groupSize);
				sliceWriter.Write(groupName);
				var groupNameSlice = sliceWriter.CreateSlice();
				var counterIdBytes = GetOrCreateCounterId(groupNameSlice, counterName);
				UpdateGroups(counterName, counterIdBytes, serverId, groupNameSlice);

				sliceWriter = new SliceWriter(buffer.FullCounterName);
				sliceWriter.WriteBytes(counterIdBytes);
				sliceWriter.WriteBytes(serverId.ToByteArray());
				sliceWriter.Write(sign);
				var counterKeySlice = sliceWriter.CreateSlice();

				storeAction(counterKeySlice);

				RemoveOldEtagIfNeeded(counterKeySlice);
				UpdateCounterMetadata(counterKeySlice);

				return false;


				


				/*var counterNameSize = Encoding.UTF8.GetByteCount(counterName);
				var fullCounterNameSize = groupSize + 
										  (sizeof(byte) * 3) + 
										  counterNameSize + 
									      32 + 
										  sizeof(byte);

				sliceWriter = GetFullCounterNameAsSliceWriter(buffer.FullCounterName,
					groupName,
					counterName,
					serverId,
					sign,
					fullCounterNameSize);

				var groupAndCounterNameSlice = sliceWriter.CreateSlice(groupSize + counterNameSize + (2*sizeof (byte)));
				//var doesCounterExist = DoesCounterExist(groupAndCounterNameSlice);
				var groupKey = sliceWriter.CreateSlice(groupSize);
				if (serverId.Equals(parent.tombstoneId))
				{
					//if it's a tombstone, we can remove the counter from the GroupsToCounters Tree
					GetOrCreateCounterId(groupKey, counterName);

					/*var readResult = countersGroups.Read(groupKey);
					if (readResult != null)
					{
						if (readResult.Reader.ReadLittleEndianInt64() == 1)
							countersGroups.Delete(groupKey);
						else
							countersGroups.Increment(groupKey, -1);	
					}
					groupAndCounterName.Delete(groupAndCounterNameSlice);#1#
				}
				else if (doesCounterExist == false)
				{
					//if the counter doesn't exist we need to update the appropriate trees
					groupToCounters.MultiAdd(groupKey, counterName);
					/*countersGroups.Increment(groupKey, 1);
					groupAndCounterName.Add(groupAndCounterNameSlice, new byte[0]);

					DeleteExistingTombstone(groupName, counterName, fullCounterNameSize);#1#
				}*/

				//save counter full name and its value into the counters tree
				
			}

			private void UpdateGroups(string counterName, byte[] counterId, Guid serverId, Slice groupNameSlice)
			{
				var sliceWriter = new SliceWriter(Encoding.UTF8.GetByteCount(counterName) + sizeof(long));
				sliceWriter.Write(counterName);
				sliceWriter.WriteBytes(counterId);
				var counterWithIdSlice = sliceWriter.CreateSlice();

				if (serverId.Equals(parent.tombstoneId))
				{
					//if it's a tombstone, we can remove the counter from the GroupsToCounters Tree
					//groupToCounters.MultiDelete(groupNameSlice, counterWithIdSlice);
					//if (groupToCounters.MultiCount(groupNameSlice) == 1)
					//	groupToCounters.Delete(groupNameSlice);
				}
				else
				{
					//if the counter doesn't exist we need to update the appropriate trees
					groupToCounters.MultiAdd(groupNameSlice, counterWithIdSlice);
					/*countersGroups.Increment(groupKey, 1);
					groupAndCounterName.Add(groupAndCounterNameSlice, new byte[0]);*/

					DeleteExistingTombstone(counterId);
				}

				sliceWriter.ResetSliceWriter();
				sliceWriter.WriteBytes(counterId);
				sliceWriter.Write(counterName);
				var idWithCounterNameSlice = sliceWriter.CreateSlice();
				counterIdWithNameToGroup.Add(idWithCounterNameSlice, groupNameSlice);
			}

			private byte[] GetOrCreateCounterId(Slice groupNameSlice, string counterName)
			{
				using (var it = groupToCounters.MultiRead(groupNameSlice))
				{
					it.RequiredPrefix = counterName;
					if (it.Seek(it.RequiredPrefix) == false || it.CurrentKey.Size != it.RequiredPrefix.Size + sizeof (long))
					{
						parent.lastCounterId++;
						EndianBitConverter.Big.CopyBytes(parent.lastCounterId, buffer.CounterId, 0);
						var slice = new Slice(buffer.CounterId);
						metadata.Add("lastCounterId", slice);
						return buffer.CounterId;
					}

					var valueReader = it.CurrentKey.CreateReader();
					valueReader.Skip(it.RequiredPrefix.Size);
					valueReader.Read(buffer.CounterId, 0, sizeof(long));
					return buffer.CounterId;
					//there is a counter named counterName in the group
					/*groupToCounters.MultiDelete(groupKey, counterName);
					if (groupToCounters.MultiCount(groupKey) == 1)
						groupToCounters.Delete(groupKey);*/
				}
			}

			private void DeleteExistingTombstone(byte[] counterId)
			{
				/*var sliceWriter = new SliceWriter(buffer.FullTombstoneName);
				sliceWriter.Write(counterId);
				sliceWriter.WriteBytes(parent.tombstoneId.ToByteArray());
				sliceWriter.Write(ValueSign.Positive);
				var tombstoneSlice = sliceWriter.CreateSlice();*/
				var tombstoneNameSlice = GetTombstoneSlice(buffer.FullTombstoneName, counterId, parent.tombstoneId);
				var tombstone = counters.Read(tombstoneNameSlice);
				if (tombstone == null)
					return;

				//delete the tombstone from the tombstones tree
				
				/*var ticks = tombstone.Reader.ReadBigEndianInt64();
				var sliceWriter = new SliceWriter(buffer.TombstoneTicks);
				sliceWriter.Write(ticks);
				var slice = sliceWriter.CreateSlice();*/

				//var slice = tombstone.Reader.AsSlice();
				var t = tombstone.Reader.ReadLittleEndianInt64();
				EndianBitConverter.Little.CopyBytes(t, buffer.TombstoneTicks, 0);
				Array.Reverse(buffer.TombstoneTicks);
				var slice = new Slice(buffer.TombstoneTicks);
				using (var it = tombstonesByDate.MultiRead(slice))
				{
					var counterIdSlice = new Slice(counterId);
					//it.Seek(Slice.BeforeAllKeys);
					it.RequiredPrefix = counterIdSlice;
					if (it.Seek(it.RequiredPrefix) == false)
						return;

					tombstonesByDate.MultiDelete(slice, it.CurrentKey);
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
				var sliceWriter = new SliceWriter(buffer.Etag);
				sliceWriter.Write(parent.lastEtag);
				var newEtagSlice = sliceWriter.CreateSlice();
				/*EndianBitConverter.Big.CopyBytes(parent.lastEtag, , 0);
				var newEtagSlice = new Slice(buffer.Etag);*/
				etagsToCounters.Add(newEtagSlice, counterKey);
				countersToEtag.Add(counterKey, newEtagSlice);
			}

			/*private static SliceWriter GetFullCounterNameAsSliceWriter(ref byte[] buffer, string groupName, string counterName, Guid serverId, string sign, int fullCounterNameSize)
			{
				EnsureBufferSize(ref buffer, fullCounterNameSize);

				var sliceWriter = new SliceWriter(buffer);				
				sliceWriter.Write(groupName);
				sliceWriter.Write(Constants.Counter.Separator);
				sliceWriter.Write(counterName);
				sliceWriter.Write(Constants.Counter.Separator);
				sliceWriter.Write(serverId.ToString());
				sliceWriter.Write(Constants.Counter.Separator);
				sliceWriter.Write(sign);
				return sliceWriter;
			}*/

			public CounterChangeAction Reset(string groupName, string counterName)
			{
				var doesCounterExist = DoesCounterExist(groupName, counterName);
				if (doesCounterExist == false)
					throw new InvalidOperationException(string.Format("Counter doesn't exist. Group: {0}, Counter Name: {1}", groupName, counterName));

				return ResetCounterInternal(groupName, counterName);
			}

			private CounterChangeAction ResetCounterInternal(string groupName, string counterName)
			{
				var difference = GetCounterTotal(groupName, counterName);
				if (difference == 0)
					return CounterChangeAction.None;

				difference = -difference;
				var counterChangeAction = Store(groupName, counterName, difference);
				return counterChangeAction;
			}

			public void Delete(string groupName, string counterName)
			{
				var counterExists = DoesCounterExist(groupName, counterName);
				if (counterExists == false)
					throw new InvalidOperationException(string.Format("Counter doesn't exist. Group: {0}, Counter Name: {1}", groupName, counterName));

				ResetCounterInternal(groupName, counterName);
				Store(groupName, counterName, parent.tombstoneId, ValueSign.Positive, counterKeySlice =>
				{
					//counter value is little endian
					EndianBitConverter.Little.CopyBytes(DateTime.Now.Ticks, buffer.CounterValue, 0);
					var counterValueSlice = new Slice(buffer.CounterValue);
					counters.Add(counterKeySlice, counterValueSlice);

					//EndianBitConverter.Big.CopyBytes(DateTime.Now.Ticks, , 0);
					//all keys are big endian
					Array.Reverse(buffer.CounterValue);
					//EndianBitConverter.Little.CopyBytes(DateTime.Now.Ticks, buffer.TombstoneTicks, 0);
					var tombstoneKeySlice = new Slice(buffer.CounterValue);
					tombstonesByDate.MultiAdd(tombstoneKeySlice, counterKeySlice);
				});
			}

			public void RecordLastEtagFor(Guid serverId, long lastEtag)
			{
				var serverIdSlice = new Slice(serverId.ToByteArray());
				EndianBitConverter.Big.CopyBytes(lastEtag, buffer.Etag, 0);
				var etagSlice = new Slice(buffer.Etag);
				serversLastEtag.Add(serverIdSlice, etagSlice);
			}

			public long GetSingleCounterValue(string groupName, string counterName, Guid serverId, char sign)
			{
				using (var it = groupToCounters.MultiRead(groupName))
				{
					it.RequiredPrefix = counterName;
					if (it.Seek(it.RequiredPrefix) == false || it.CurrentKey.Size != it.RequiredPrefix.Size + sizeof(long))
						return -1;

					var valueReader = it.CurrentKey.CreateReader();
					valueReader.Read(buffer.CounterId, 0, sizeof(long));

					var sliceWriter = new SliceWriter(buffer.FullCounterName);
					sliceWriter.WriteBytes(buffer.CounterId);
					sliceWriter.WriteBytes(serverId.ToByteArray());
					sliceWriter.Write(sign);
					return reader.GetSingleCounterValue(sliceWriter.CreateSlice());
				}
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

			/*private bool DoesCounterExist(Slice groupWithCounterName)
			{
				using (var it = groupAndCounterName.Iterate())
				{
					it.RequiredPrefix = groupWithCounterName;
					return it.Seek(groupWithCounterName);
				}
			}*/

			public bool PurgeOutdatedTombstones()
			{
				var timeAgo = DateTime.Now.AddTicks(-parent.tombstoneRetentionTime.Ticks);
				EndianBitConverter.Big.CopyBytes(timeAgo.Ticks, buffer.TombstoneTicks, 0);
				var tombstone = new Slice(buffer.TombstoneTicks);

				var counterNameBuffer = new byte[0];
				using (var it = tombstonesByDate.Iterate())
				{
					it.RequiredPrefix = tombstone;
					if (it.Seek(it.RequiredPrefix) == false)
						return false;

					do
					{
						using (var iterator = tombstonesByDate.MultiRead(it.CurrentKey))
						{
							var valueReader = iterator.CurrentKey.CreateReader();
							valueReader.Read(buffer.CounterId, 0, iterator.CurrentKey.Size - sizeof(long));
							valueReader.Reset();
							EnsureBufferSize(ref counterNameBuffer, iterator.CurrentKey.Size);
							valueReader.Read(counterNameBuffer, 0, iterator.CurrentKey.Size);
							DeleteCounterById(buffer.CounterId, counterNameBuffer);

							tombstonesByDate.MultiDelete(it.CurrentKey, iterator.CurrentKey);
							if (groupToCounters.MultiCount(it.CurrentKey) == 1)
								tombstonesByDate.Delete(it.CurrentKey);
						}

						//TODO: delete only configurable amount of counters
					} while (it.MoveNext());
				}

				return true;
			}

			private void DeleteCounterById(byte[] counterIdBuffer, byte[] counterIdWithNameBuffer)
			{
				var slice = new Slice(counterIdWithNameBuffer);
				counterIdWithNameToGroup.Delete(slice);

				using (var it = counters.Iterate())
				{
					var counterIdSlice = new Slice(counterIdBuffer);
					it.RequiredPrefix = counterIdSlice;
					if (it.Seek(it.RequiredPrefix) == false)
						return;

					do
					{
						var counterKey = it.CurrentKey;
						RemoveOldEtagIfNeeded(counterKey);
						countersToEtag.Delete(counterKey);
						counters.Delete(counterKey);
					} while (it.MoveNext());
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
				//parent.LastWrite = SystemTime.UtcNow;
				if (transaction != null)
					transaction.Dispose();
			}
		}

		private static void EnsureBufferSize(ref byte[] buffer, int requiredBufferSize)
		{
			if (buffer.Length < requiredBufferSize)
				buffer = new byte[Utils.NearestPowerOfTwo(requiredBufferSize)];
		}

		private static long CalculateCounterTotal(Counter counterValuesByPrefix)
		{
			long sum = 0;
			// ReSharper disable once LoopCanBeConvertedToQuery
			foreach (var x in counterValuesByPrefix.CounterValues)
				sum += x.IsPositive() ? x.Value : -x.Value;
			return sum;
		}

		private static Slice GetTombstoneSlice(byte[] tombstoneBuffer, byte[] counterIdBytes, Guid tombstoneId)
		{
			var sliceWriter = new SliceWriter(tombstoneBuffer);
			sliceWriter.WriteBytes(counterIdBytes);
			sliceWriter.WriteBytes(tombstoneId.ToByteArray());
			sliceWriter.Write(ValueSign.Positive);
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
			public const string GroupToCounters = "group->counters";
			public const string CounterIdWithNameToGroup = "counterIdWithName->group";
			public const string CountersToEtag = "counters->etags";
			public const string EtagsToCounters = "etags->counters";
			public const string Metadata = "$metadata";
		}
	}
}