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
using Raven.Abstractions.Util;
using Raven.Database.Config;
using Raven.Database.Counters.Controllers;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Connections;
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
        public string CounterStorageUrl { get; private set; }
        private readonly StorageEnvironment storageEnvironment;
        public readonly RavenCounterReplication ReplicationTask;
		
		public DateTime LastWrite { get; private set; }

		private long lastEtag;
		private Guid Id { get; set; }
		private ConcurrentDictionary<string, Guid> serverNameToId;

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

			//TODO: add an option to create a ReplicationRequestTimeout when creating a new counter storage
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
				var serverNamesToIds = storageEnvironment.CreateTree(tx, "serverNames->Ids");
				var serverIdsToNames = storageEnvironment.CreateTree(tx, "Ids->serverNames");
				storageEnvironment.CreateTree(tx, "servers->lastEtag");
				storageEnvironment.CreateTree(tx, "counters");
				storageEnvironment.CreateTree(tx, "countersGroups");
				var etags = storageEnvironment.CreateTree(tx, "etags->counters");
				storageEnvironment.CreateTree(tx, "counters->etags");

				var metadata = tx.State.GetTree(tx, "$metadata");
				var id = metadata.Read("id");

				if (id == null) // new counter db
				{
					Id = Guid.NewGuid();
					metadata.Add("id", Id.ToByteArray());
					metadata.Add("name", Encoding.UTF8.GetBytes(Name));

					var serverIdSlice = new Slice(Id.ToByteArray());
					serverNameToId.GetOrAdd(CounterStorageUrl, Id);
					serverNamesToIds.Add(CounterStorageUrl, serverIdSlice);
					serverIdsToNames.Add(serverIdSlice, CounterStorageUrl);

					tx.Commit();
				}
				else // existing counter db
				{
					int used;
					Id = new Guid(id.Reader.ReadBytes(16, out used));
					var nameResult = metadata.Read("name");
					if (nameResult == null)
						throw new InvalidOperationException("Could not read name from the store, something bad happened");
					var storedName = new StreamReader(nameResult.Reader.AsStream()).ReadToEnd();

					if (storedName != Name)
						throw new InvalidOperationException("The stored name " + storedName + " does not match the given name " + Name);

					using (var it = etags.Iterate())
					{
						if (it.Seek(Slice.AfterAllKeys))
						{
							lastEtag = it.CurrentKey.CreateReader().ReadBigEndianInt64();
						}
					}

					using (var it = serverIdsToNames.Iterate())
					{
						do
						{
							
							var reader = it.CreateReaderForCurrent();
							serverNameToId.GetOrAdd(it.CurrentKey.ToString(), new Guid(reader.AsSlice().ToString()));
						} while (it.MoveNext());
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
                    CounterStorageSizeOnDiskInMB = ConvertBytesToMBs(GetCounterStorageSizeOnDisk()),
                    GroupsCount =  reader.GetGroupsCount(),
                    ServersCount = reader.GetServersCount()
	            };
	            return stats;
	        }
	    }


        private static decimal ConvertBytesToMBs(long bytes)
        {
            return Math.Round(bytes / 1024.0m / 1024.0m, 2);
        }

        /// <summary>
        ///     Get the total size taken by the counters storage on the disk.
        ///     This explicitly does NOT include in memory data.
        /// </summary>
        /// <remarks>
        ///     This is a potentially a very expensive call, avoid making it if possible.
        /// </remarks>
        private long GetCounterStorageSizeOnDisk()
        {
            if (storageEnvironment.Options is StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)
            {
                var directoryStorageOptions = storageEnvironment.Options as StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions;
                string[] counters = Directory.GetFiles(directoryStorageOptions.BasePath, "*.*", SearchOption.AllDirectories);
                long totalCountersSize = counters.Sum(file =>
                {
                    try
                    {
                        return new FileInfo(file).Length;
                    }
                    catch (UnauthorizedAccessException)
                    {
                        return 0;
                    }
                    catch (FileNotFoundException)
                    {
                        return 0;
                    }
                });

                return totalCountersSize;
            }
                return 0;
        }

        //todo: consider implementing metricses for each counter, not only for each counter storage
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
			return new Reader(storageEnvironment);
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
            ReplicationTask.Dispose();
			if (storageEnvironment != null)
				storageEnvironment.Dispose();
            metricsCounters.Dispose();
		}

		[CLSCompliant(false)]
		public class Reader : IDisposable
		{
			private readonly Transaction transaction;
			private readonly Tree serverNamesToIds, serverIdsToNames, serversLastEtag, counters, countersEtags, countersGroups, etagsCounters, metadata;
			private readonly byte[] serverIdBytes = new byte[sizeof(int)];

            public Reader(StorageEnvironment storageEnvironment)
                : this(storageEnvironment.NewTransaction(TransactionFlags.Read)) { }

			[CLSCompliant(false)]
            public Reader(Transaction tx)
            {
				transaction = tx;
				serverNamesToIds = transaction.State.GetTree(transaction, "serverNames->Ids");
				serverIdsToNames = transaction.State.GetTree(transaction, "Ids->serverNames");
				serversLastEtag = transaction.State.GetTree(transaction, "servers->lastEtag");
                counters = transaction.State.GetTree(transaction, "counters");
                countersGroups = transaction.State.GetTree(transaction, "countersGroups");
                countersEtags = transaction.State.GetTree(transaction, "counters->etags");
                etagsCounters = transaction.State.GetTree(transaction, "etags->counters");
				metadata = transaction.State.GetTree(transaction, "$metadata");
            }

		    public long GetCountersCount()
		    {
		        return countersEtags.State.EntriesCount;
		    }

            public long GetGroupsCount()
            {
                return countersGroups.State.EntriesCount;
            }

            public long GetServersCount()
            {
                return serverNamesToIds.State.EntriesCount;
            }

			public IEnumerable<string> GetCounterNames(string prefix)
			{
				using (var it = countersEtags.Iterate())
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

			public IEnumerable<Group> GetCounterGroups()
			{
				using (var it = countersGroups.Iterate())
				{
					if (it.Seek(Slice.BeforeAllKeys) == false)
						yield break;
					do
					{
						yield return new Group
						{
							Name = it.CurrentKey.ToString(),
							NumOfCounters = it.CreateReaderForCurrent().ReadBigEndianInt64()
						};
					} while (it.MoveNext());
				}
			}		

            public Counter GetCountersByPrefix(string namePrefix)
			{
	            var etagResult = countersEtags.Read(namePrefix);
				if (etagResult == null)
					return null;
                var etag = etagResult.Reader.ReadBigEndianInt64();
	           
	            using (var it = counters.Iterate())
				{
					it.RequiredPrefix = namePrefix + Constants.ServerIdSeperatorString;
					if (it.Seek(namePrefix) == false)
						return null;

					var result = new Counter
					{
						Etag = etag
					};
					do
					{
						it.CurrentKey.CopyTo(it.CurrentKey.Size - 4, serverIdBytes, 0, 4);
						var reader = it.CreateReaderForCurrent();
						result.ServerValues.Add(new Counter.PerServerValue
						{
							SourceId = EndianBitConverter.Big.ToInt32(serverIdBytes, 0),
                            Positive = reader.ReadBigEndianInt64(),
                            Negative = reader.ReadBigEndianInt64()
						});
					} while (it.MoveNext());
					return result;
				}
			}
            
            public IEnumerable<ReplicationCounter> GetCountersSinceEtag(long etag)
		    {
                var buffer = new byte[sizeof(long)];
                EndianBitConverter.Big.CopyBytes(etag, buffer, 0);
		        var slice = new Slice(buffer);
                
                using (var it = etagsCounters.Iterate())
                {
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
                        var counterName = Encoding.UTF8.GetString(buffer, 0, currentDataSize);

                        var counter = GetCountersByPrefix(counterName);
                        yield return new ReplicationCounter
                        {
                            CounterName = counterName,
                            Etag = counter.Etag,
                            ServerValues = counter.ServerValues.Select(x => new ReplicationCounter.PerServerValue
                            {
                                ServerName = ServerNameFor(x.SourceId),
                                Positive = x.Positive,
                                Negative = x.Negative
                            }).ToList()
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

                        it.CurrentKey.CopyTo(0, serverIdBytes, 0, 4);
                        it.CreateReaderForCurrent().Read(buffer, 0, buffer.Length);                        
                        yield return new ServerEtag
                        {
                            SourceId = EndianBitConverter.Big.ToInt32(serverIdBytes, 0),
                            Etag = EndianBitConverter.Big.ToInt64(buffer, 0),
                        };

                    } while (it.MoveNext());
                }
		    }

			private int GetServerId(string server)
			{
				throw new NotImplementedException();
				/*int serverId = -1;
				var key = Encoding.UTF8.GetBytes(server);
				var result = serverNamesToIds.Read(new Slice(key));

				if (result != null && result.Version != 0)
				{
					serverId = result.Reader.ReadBigEndianInt32();
				}

				return serverId;*/
			}

			public long GetLastEtagFor(string server)
			{
				throw new NotImplementedException();
				/*long serverEtag = 0;
				int serverId = GetServerId(server);
				if (serverId == -1)
				{
					return serverEtag;
				}

				var key = EndianBitConverter.Big.GetBytes(serverId);
				var result = serversLastEtag.Read(new Slice(key));
				if (result != null && result.Version != 0)
				{
					serverEtag = result.Reader.ReadBigEndianInt64();
				}

				return serverEtag;*/
			}

            public CounterStorageReplicationDocument GetReplicationData()
			{
				var readResult = metadata.Read("replication");
				if (readResult != null)
				{
					var stream = readResult.Reader.AsStream();
					stream.Position = 0;
					using (var streamReader = new StreamReader(stream))
					using (var jsonTextReader = new JsonTextReader(streamReader))
					{
                        return new JsonSerializer().Deserialize<CounterStorageReplicationDocument>(jsonTextReader);
					}
				}
				return null;
			}

			public string ServerNameFor(int serverId)
			{
				throw new NotImplementedException();
				/*string serverName = string.Empty;

				var key = EndianBitConverter.Big.GetBytes(serverId);
				var result = serverIdsToNames.Read(new Slice(key));

				if (result != null && result.Version != 0)
				{
					serverName = result.Reader.AsSlice().ToString();
				}

				return serverName;*/
			}

			public int SourceIdFor(string serverName)
			{
				throw new NotImplementedException();
				/*int serverId = 0;
				var key = Encoding.UTF8.GetBytes(serverName);
				var result = serverNamesToIds.Read(new Slice(key));

				if (result != null && result.Version != 0)
				{
					serverId = result.Reader.ReadBigEndianInt32();
				}

				return serverId;*/
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
			private readonly Tree serverNamesToIds, serverIdsToNames, serversLastEtag, counters, etagsCountersIx, countersEtagIx, countersGroups, metadata;
            private readonly byte[] tempThrowawayBuffer;
			private byte[] counterFullNameInBytes = new byte[0];
			private readonly byte[] etagBuffer = new byte[sizeof(long)];
		    private readonly Reader reader;
			private readonly int storeBufferLength;
			private WriteBatch writeBatch;
			private SnapshotReader snapshotReader;

			public Writer(CounterStorage parent, StorageEnvironment storageEnvironment)
			{
				this.parent = parent;
				transaction = storageEnvironment.NewTransaction(TransactionFlags.Read);
				writeBatch = new WriteBatch();
				snapshotReader = new SnapshotReader(transaction);
                reader = new Reader(transaction);
				serverNamesToIds = transaction.State.GetTree(transaction, "serverNames->Ids");
				serverIdsToNames = transaction.State.GetTree(transaction, "Ids->serverNames");
				serversLastEtag = transaction.State.GetTree(transaction, "servers->lastEtag");
                counters = transaction.State.GetTree(transaction, "counters");
				countersGroups = transaction.State.GetTree(transaction, "countersGroups");
				etagsCountersIx = transaction.State.GetTree(transaction, "etags->counters");
				countersEtagIx = transaction.State.GetTree(transaction, "counters->etags");
				metadata = transaction.State.GetTree(transaction, "$metadata");

				tempThrowawayBuffer = new byte[sizeof(long) + //positive
									   sizeof(long)]; // negative

				storeBufferLength = tempThrowawayBuffer.Length;
			}

            public Counter GetCountersByPrefix(string name)
            {
                return reader.GetCountersByPrefix(name);
            }

			public long GetLastEtagFor(string serverName)
			{
				return reader.GetLastEtagFor(serverName);
			}

			public int SourceIdFor(string serverName)
			{
				return reader.SourceIdFor(serverName);
			}

			public void Store(string serverName, string fullCounterName, long delta)
		    {
				Store(serverName, fullCounterName, result =>
		        {
                    
		            int valPos = 0;
		            if (delta < 0)
		            {
		                valPos = 8;
		                delta = -delta;
                        parent.MetricsCounters.DecSizeMetrics.Update(delta);
                        parent.MetricsCounters.Decrements.Mark();
		            }
		            else
		            {
                        parent.MetricsCounters.IncSizeMetrics.Update(delta);
                        parent.MetricsCounters.Increments.Mark();                        
		            }

		            if (result == null)
		            {
		                EndianBitConverter.Big.CopyBytes(delta, tempThrowawayBuffer, valPos);
		                EndianBitConverter.Big.CopyBytes(0L, tempThrowawayBuffer, valPos == 0 ? 8 : 0);
		            }
		            else
		            {
						result.Reader.Read(tempThrowawayBuffer, 0, storeBufferLength);
		                delta += EndianBitConverter.Big.ToInt64(tempThrowawayBuffer, valPos);
		                EndianBitConverter.Big.CopyBytes(delta, tempThrowawayBuffer, valPos);
		            }
		        });
		    }

			public void Store(string serverName, string fullCounterName, long positive, long negative)
            {
				Store(serverName, fullCounterName, result =>
                {
                    EndianBitConverter.Big.CopyBytes(positive, tempThrowawayBuffer, 0);
                    EndianBitConverter.Big.CopyBytes(negative, tempThrowawayBuffer, 8);
                });
            }

			private void Store(string serverName, string fullCounterName,/* bool isIncrement, */Action<ReadResult> setStoreBuffer)
			{
                var lastEtag = Interlocked.Increment(ref parent.lastEtag);
				var serverId = GetOrAddServerId(serverName);

				var counterNameSize = Encoding.UTF8.GetByteCount(fullCounterName);
				var requiredBufferSize = counterNameSize + sizeof(int) + 1;
				EnsureBufferSize(requiredBufferSize);
				Debug.Assert(requiredBufferSize < ushort.MaxValue);

				var sliceWriter = new SliceWriter(counterFullNameInBytes);
				sliceWriter.WriteString(fullCounterName);
				sliceWriter.WriteString(Constants.ServerIdSeperatorString);
				sliceWriter.WriteString(serverId);

				var endOfGroupNameIndex = Array.IndexOf(counterFullNameInBytes, Constants.GroupSeparator, 0, counterNameSize);
				if (endOfGroupNameIndex == -1)
					throw new InvalidOperationException("Could not find group name in counter, no separator");
				
				var counterKey = sliceWriter.CreateSlice();
				var counterReadResult = snapshotReader.Read(counters.Name, counterKey, writeBatch); //= counters.Read(counterKey);
				var counterFullNamePrefix = fullCounterName + Constants.ServerIdSeperatorString;
				if (counterReadResult == null && !IsCounterExists(counterFullNamePrefix)) //it's a new counter
				{
					var groupKey = new Slice(counterFullNameInBytes, (ushort)endOfGroupNameIndex);
					var groupReadResult = snapshotReader.Read(countersGroups.Name, groupKey, writeBatch);
					if (groupReadResult == null)
					{
						writeBatch.Increment(groupKey, 1, countersGroups.Name, shouldIgnoreConcurrencyExceptions: true);
					}
					else
					{
						writeBatch.Increment(groupKey, 1, countersGroups.Name, groupReadResult.Version, true);
					}
				}

				


				/*if (counterReadResult == null && !IsCounterExists(fullCounterName)) //if it's a new counter
				{
				    var curGroupReadResult = countersGroups.Read(groupKey);
                    long currentValue = 0;
				    if (curGroupReadResult != null)
				    {
                        currentValue = curGroupReadResult.Reader.ReadBigEndianInt64();
                        countersGroups.Add(groupKey, new Slice(EndianBitConverter.Big.GetBytes(currentValue)));
				    }
				    else
				    {
                        countersGroups.Add(groupKey, new Slice(EndianBitConverter.Big.GetBytes(currentValue)));
				    }
					//countersGroups.Increment(groupKeySlice, 1); todo: consider return that after pavel's fix will be added
				}*/



				setStoreBuffer(counterReadResult);

				counters.Add(counterKey, tempThrowawayBuffer);

				counterKey = new Slice(counterFullNameInBytes, (ushort) counterNameSize);
				counterReadResult = countersEtagIx.Read(counterKey);
				
				if (counterReadResult != null) // remove old etag entry
				{
					counterReadResult.Reader.Read(etagBuffer, 0, sizeof (long));
                    var oldEtagSlice = new Slice(etagBuffer);
                    etagsCountersIx.Delete(oldEtagSlice);
				}
                
				EndianBitConverter.Big.CopyBytes(lastEtag, etagBuffer, 0);
                var newEtagSlice = new Slice(etagBuffer);

				
                etagsCountersIx.Add(newEtagSlice, counterKey);
                countersEtagIx.Add(counterKey, newEtagSlice);
			}

			public bool Reset(string serverName, string fullCounterName)
			{
				var counter = GetCountersByPrefix(fullCounterName); //TODO: implement get counter without an etag
				if (counter == null)
					return false;

				var overallTotalPositive = counter.ServerValues.Sum(x => x.Positive);
				var overallTotalNegative = counter.ServerValues.Sum(x => x.Negative);
				var difference = overallTotalPositive - overallTotalNegative;

				if (difference != 0)
				{
					difference = -difference;
					Store(serverName, fullCounterName, difference);
					parent.MetricsCounters.Resets.Mark();
					return true;
				}
				return false;
			}

			public void RecordLastEtagFor(string serverName, long lastEtag)
			{
				var serverId = GetOrAddServerId(serverName);
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

			private void EnsureBufferSize(int requiredBufferSize)
			{
				if (counterFullNameInBytes.Length < requiredBufferSize)
					counterFullNameInBytes = new byte[Utils.NearestPowerOfTwo(requiredBufferSize)];
			}

			 

			private string GetOrAddServerId(string serverName)
			{
				var serverId = parent.serverNameToId.GetOrAdd(serverName, arg => Guid.NewGuid());
				var serverIdString = serverId.ToString();

				var result = snapshotReader.Read(serverNamesToIds.Name, serverName, writeBatch);
				if (result == null)
				{
					writeBatch.Add(serverName, serverIdString, serverNamesToIds.Name, shouldIgnoreConcurrencyExceptions: true);
				}

				result = snapshotReader.Read(serverIdsToNames.Name, serverIdString, writeBatch);
				if (result == null)
				{
					writeBatch.Add(serverIdString, serverName, serverIdsToNames.Name, shouldIgnoreConcurrencyExceptions: true);
				}

				return serverIdString;
				/*long serverId;
				var result = serverNamesToIds.Read(server));

				if (result != null && result.Version != 0)
				{
					serverId = result.Reader.ReadBigEndianInt64();
				}
				else
				{
					serverId = Interlocked.Read(ref parent.numberOfServers);
					var serverIdBytes = EndianBitConverter.Big.GetBytes(serverId);

					//serverId = (int)serverNamesToIds.State.EntriesCount; //todo: should we check for overflow or change the server id to long?
					//var serverIdBytes = EndianBitConverter.Big.GetBytes(serverId);
					var serverIdSlice = new Slice(serverIdBytes);
					serverNamesToIds.Add(serverName, serverIdSlice);
					serverIdsToNames.Add(serverIdSlice, server);
				}

				return serverId;*/
			}

			private bool IsCounterExists(Slice name)
			{
				using (var it = counters.Iterate())
				{
					it.RequiredPrefix = name;
					return it.Seek(name);
				}
			}

			public void Commit(bool notifyParent = true)
			{
				this.parent.storageEnvironment.Writer.Write(writeBatch);
				transaction.Commit();
				if (notifyParent)
				{
					parent.Notify();
				}
			}

			public void Dispose()
			{
				writeBatch.Dispose();
				snapshotReader.Dispose();
				parent.LastWrite = SystemTime.UtcNow;
                if (transaction != null)
					transaction.Dispose();
			}
		}

	    public class ServerEtag
	    {
	        public int SourceId { get; set; }
	        public long Etag { get; set; }
	    }

        string IResourceStore.Name
        {
            get { return Name; }
        }
    }
}