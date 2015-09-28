using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
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
			deletedTombstonesInBatch = configuration.Counter.DeletedTombstonesInBatch;
			metricsCounters = new CountersMetricsManager();
			Configuration = configuration;
			ExtensionsState = new AtomicDictionary<object>();
			jsonSerializer = new JsonSerializer();
			sizeOfGuid = sizeof(Guid);

			Initialize();
			//purgeTombstonesTimer = new Timer(BackgroundActionsCallback, null, TimeSpan.Zero, TimeSpan.FromHours(1));
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

		public StorageEnvironment Environment
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
					CounterStorageSize = SizeHelper.Humane(Environment.Stats().UsedDataFileSizeInBytes),
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

			exceptionAggregator.ThrowIfNeeded();
		}

		[CLSCompliant(false)]
		public class Reader : IDisposable
		{
			private readonly Transaction transaction;
			private readonly Tree counters, tombstonesByDate, groupToCounters, tombstonesGroupToCounters, counterIdWithNameToGroup, etagsToCounters, countersToEtag, serversLastEtag, replicationSources, metadata;
			private readonly CounterStorage parent;

			[CLSCompliant(false)]
			public Reader(CounterStorage parent, Transaction transaction)
			{
				this.transaction = transaction;
				this.parent = parent;
				counters = transaction.ReadTree(TreeNames.Counters);
				tombstonesByDate = transaction.ReadTree(TreeNames.DateToTombstones);
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
						skip -= (int)countersInGroup; //TODO: is there a better way?
					} while (it.MoveNext());

					var isEmptyGroup = groupName.Equals(string.Empty);
					do
					{
						using (var iterator = groupToCounters.MultiRead(it.CurrentKey))
						{
							if (iterator.Seek(Slice.BeforeAllKeys) == false || (skip > 0 && iterator.Skip(skip) == false))
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

			public List<CounterSummary> GetCountersSummary(string groupName, int skip = 0, int take = int.MaxValue)
			{
				ThrowIfDisposed();
				var countersDetails = GetCountersDetails(groupName, skip).Take(take);
				var serverIdBuffer = new byte[parent.sizeOfGuid];
				return countersDetails.Select(counterDetails => new CounterSummary
				{
					GroupName = counterDetails.Group,
					CounterName = counterDetails.Name,
					Increments = CalculateCounterTotalChangeBySign(counterDetails.IdSlice, serverIdBuffer, ValueSign.Positive),
					Decrements = CalculateCounterTotalChangeBySign(counterDetails.IdSlice, serverIdBuffer, ValueSign.Negative)
				}).ToList();
			}

			private long CalculateCounterTotalChangeBySign(Slice counterIdSlice, byte[] serverIdBuffer, char signToCalculate)
			{
				using (var it = counters.Iterate())
				{
					it.RequiredPrefix = counterIdSlice;
					var seekResult = it.Seek(it.RequiredPrefix);
					//should always be true
					Debug.Assert(seekResult == true);

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

			public long GetCounterTotal(string groupName, string counterName)
			{
				ThrowIfDisposed();
				using (var it = groupToCounters.MultiRead(groupName))
				{
					it.RequiredPrefix = counterName;
					if (it.Seek(it.RequiredPrefix) == false || it.CurrentKey.Size != it.RequiredPrefix.Size + sizeof(long))
					{
						var e = new InvalidDataException("Counter doesn't exist!");
						e.Data.Add("DoesntExist", true);
						throw e;
					}

					var valueReader = it.CurrentKey.CreateReader();
					valueReader.Skip(it.RequiredPrefix.Size);
					int used;
					var counterIdBuffer = valueReader.ReadBytes(sizeof(long), out used);
					var slice = new Slice(counterIdBuffer, sizeof(long));
					return CalculateCounterTotal(slice, new byte[parent.sizeOfGuid]);
				}
			}

			public IEnumerable<CounterGroup> GetCounterGroups()
			{
				ThrowIfDisposed();
				using (var it = groupToCounters.Iterate())
				{
					if (it.Seek(Slice.BeforeAllKeys) == false)
						yield break;

					do
					{
						yield return new CounterGroup
						{
							Name = it.CurrentKey.ToString(),
							Count = groupToCounters.MultiCount(it.CurrentKey)
						};
					} while (it.MoveNext());
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
					//should always be true
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

						var serverValue = new ServerValue { ServerId = serverId };
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

			public IEnumerable<ServerEtagAndSourceName> GetServerSources()
			{
				ThrowIfDisposed();
				var lookupDict = GetServerEtags().ToDictionary(x => x.ServerId, x => x.Etag);

				using (var it = replicationSources.Iterate())
				{
					if (it.Seek(Slice.BeforeAllKeys) == false)
						yield break;

					do
					{
						var b = new byte[it.CurrentKey.Size];
						it.CurrentKey.CreateReader().Read(b, 0, b.Length);

						var serverId = new Guid(b);

						yield return new ServerEtagAndSourceName
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

					do
					{
						var b = new byte[it.CurrentKey.Size];
						it.CurrentKey.CreateReader().Read(b, 0, b.Length);

						yield return new ServerEtag
						{
							ServerId = new Guid(b),
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
				using (var it = counterIdWithNameToGroup.Iterate())
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

			public string GetSourceNameFor(Guid serverId)
			{
				ThrowIfDisposed();
				var slice = new Slice(serverId.ToByteArray());
				var readResult = replicationSources.Read(slice);
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
				}

				public readonly byte[] FullCounterName;
				public readonly byte[] CounterValue = new byte[sizeof(long)];
				public readonly byte[] Etag = new byte[sizeof(long)];
				public readonly byte[] CounterId = new byte[sizeof(long)];
				public readonly Lazy<byte[]> TombstoneTicks = new Lazy<byte[]>(() => new byte[sizeof(long)]);

				public byte[] GroupName = new byte[0];
				public byte[] CounterNameWithId = new byte[0];
			}

			public string Name
			{
				get { return parent.Name; }
			}

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

			public long GetCounterTotal(string groupName, string counterName)
			{
				ThrowIfDisposed();
				return reader.GetCounterTotal(groupName, counterName);
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
				var groupNameSlice = CreateGroupNameSlice(groupName);
				var counterIdBuffer = GetCounterIdBufferFromTree(groupToCounters, groupNameSlice, counterName);
				var doesCounterExist = counterIdBuffer != null;
				if (doesCounterExist == false)
				{
					counterIdBuffer = GetCounterIdBufferFromTree(tombstonesGroupToCounters, groupNameSlice, counterName);
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
				EnsureBufferSize(ref buffer.CounterNameWithId, counterNameWithIdSize);
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

			private byte[] GetCounterIdBufferFromTree(Tree tree, Slice groupNameSlice, string counterName)
			{
				using (var it = tree.MultiRead(groupNameSlice))
				{
					it.RequiredPrefix = counterName;
					if (it.Seek(it.RequiredPrefix) == false || it.CurrentKey.Size != it.RequiredPrefix.Size + sizeof(long))
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
				if (tombstone == null)
					return;

				//delete the tombstone from the tombstones tree
				tombstone.Reader.Read(buffer.TombstoneTicks.Value, 0, sizeof(long));
				Array.Reverse(buffer.TombstoneTicks.Value);
				var slice = new Slice(buffer.TombstoneTicks.Value);
				dateToTombstones.MultiDelete(slice, tombstoneNameSlice);

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
					throw new InvalidOperationException(string.Format("Counter doesn't exist. Group: {0}, Counter Name: {1}", groupName, counterName));

				return ResetCounterInternal(groupName, counterName);
			}

			private long ResetCounterInternal(string groupName, string counterName)
			{
				var difference = GetCounterTotal(groupName, counterName);
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
				var groupNameSlice = CreateGroupNameSlice(groupName);
				var counterIdBuffer = GetCounterIdBufferFromTree(groupToCounters, groupNameSlice, counterName);
				var singleCounterValue = new SingleCounterValue { DoesCounterExist = counterIdBuffer != null };

				if (counterIdBuffer == null)
				{
					//looking for the single counter in the tombstones tree
					counterIdBuffer = GetCounterIdBufferFromTree(tombstonesGroupToCounters, groupNameSlice, counterName);
					if (counterIdBuffer == null)
						return singleCounterValue;
				}

				var fullCounterNameSlice = GetFullCounterNameSlice(counterIdBuffer, serverId, sign);
				singleCounterValue.Value = reader.GetSingleCounterValue(fullCounterNameSlice);
				return singleCounterValue;
			}

			private Slice CreateGroupNameSlice(string groupName)
			{
				var groupSize = Encoding.UTF8.GetByteCount(groupName);
				Debug.Assert(groupSize < ushort.MaxValue);
				EnsureBufferSize(ref buffer.GroupName, groupSize);
				var sliceWriter = new SliceWriter(buffer.GroupName);
				sliceWriter.Write(groupName);
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

			public bool PurgeOutdatedTombstones()
			{
				ThrowIfDisposed();
				var timeAgo = DateTime.Now.AddTicks(-parent.tombstoneRetentionTime.Ticks);
				EndianBitConverter.Big.CopyBytes(timeAgo.Ticks, buffer.TombstoneTicks.Value, 0);
				var tombstone = new Slice(buffer.TombstoneTicks.Value);
				var deletedTombstonesInBatch = parent.deletedTombstonesInBatch;
				using (var it = dateToTombstones.Iterate())
				{
					it.RequiredPrefix = tombstone;
					if (it.Seek(it.RequiredPrefix) == false)
						return false;

					do
					{
						using (var iterator = dateToTombstones.MultiRead(it.CurrentKey))
						{
							var valueReader = iterator.CurrentKey.CreateReader();
							valueReader.Read(buffer.CounterId, 0, iterator.CurrentKey.Size - sizeof(long));
							DeleteCounterById(buffer.CounterId);
							dateToTombstones.MultiDelete(it.CurrentKey, iterator.CurrentKey);
						}
					} while (it.MoveNext() && --deletedTombstonesInBatch > 0);
				}

				return true;
			}

			private void DeleteCounterById(byte[] counterIdBuffer)
			{
				var counterIdSlice = new Slice(counterIdBuffer);
				using (var it = counterIdWithNameToGroup.Iterate())
				{
					it.RequiredPrefix = counterIdSlice;
					var seek = it.Seek(it.RequiredPrefix);
					Debug.Assert(seek == true);

					var counterNameSlice = GetCounterNameSlice(it.CurrentKey.Size, it.CurrentKey.CreateReader());
					var valueReader = it.CreateReaderForCurrent();
					var groupNameSlice = valueReader.AsSlice();

					tombstonesGroupToCounters.MultiDelete(groupNameSlice, counterNameSlice);
					counterIdWithNameToGroup.Delete(it.CurrentKey);
				}

				//remove all counters values for all servers
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
						counters.Delete(counterKey);
					} while (it.MoveNext());
				}
			}

			private static Slice GetCounterNameSlice(ushort currentKeySize, ValueReader currentReader)
			{
				var counterNameSize = currentKeySize - sizeof(long);
				//EnsureBufferSize(ref buffer.CounterNameBuffer, counterNameSize);
				//var currentReader = it.CurrentKey.CreateReader();
				currentReader.Skip(sizeof(long));
				//currentReader.Read(buffer.CounterNameBuffer, 0, counterNameSize);
				int used;
				var counterNameBuffer = currentReader.ReadBytes(counterNameSize, out used);
				var counterNameSlice = new Slice(counterNameBuffer, (ushort)counterNameSize);
				return counterNameSlice;
			}

			private static void EnsureBufferSize(ref byte[] buffer, int requiredBufferSize)
			{
				if (buffer.Length < requiredBufferSize)
					buffer = new byte[Utils.NearestPowerOfTwo(requiredBufferSize)];
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
				if (transaction != null)
					transaction.Dispose();
			}

			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			[Conditional("DEBUG")]
			private void ThrowIfDisposed()
			{
				if (transaction.IsDisposed)
					throw new ObjectDisposedException("CounterStorage::Reader", "The reader should not be used after being disposed.");
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

		public class ServerEtagAndSourceName : ServerEtag
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