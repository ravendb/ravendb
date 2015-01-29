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

		private string ServerId { get; set; }
		private ConcurrentDictionary<string, string> ServerNamesToIds { get; set; }
		private ConcurrentDictionary<string, string> ServerIdsToNames { get; set; }

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
			ServerNamesToIds = new ConcurrentDictionary<string, string>();
			ServerIdsToNames = new ConcurrentDictionary<string, string>();
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
					var serverIdGuid = Guid.NewGuid();
					ServerId = serverIdGuid.ToString();
					var serverIdBytes = Guid.NewGuid().ToByteArray();

					metadata.Add("id", serverIdBytes);
					metadata.Add("name", Encoding.UTF8.GetBytes(Name));

					var serverIdSlice = new Slice(serverIdBytes);
					ServerNamesToIds.GetOrAdd(CounterStorageUrl, ServerId);
					ServerIdsToNames.GetOrAdd(ServerId, CounterStorageUrl);
					serverNamesToIds.Add(CounterStorageUrl, serverIdSlice);
					serverIdsToNames.Add(serverIdSlice, CounterStorageUrl);

					tx.Commit();
				}
				else // existing counter db
				{
					int used;
					var serverIdGuid = new Guid(id.Reader.ReadBytes(16, out used));
					ServerId = serverIdGuid.ToString();
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
							ServerNamesToIds.GetOrAdd(it.CurrentKey.ToString(), reader.ReadBytes(16, out used).ToString());
							ServerIdsToNames.GetOrAdd(reader.ReadBytes(16, out used).ToString(), it.CurrentKey.ToString());
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
			return new Reader(this, storageEnvironment);
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
			private readonly CounterStorage parent;
			private readonly Transaction transaction;
			private readonly Tree serverNamesToIds, serverIdsToNames, serversLastEtag, counters, countersToEtags, countersGroups, etagsToCounters, metadata;
			private readonly byte[] serverIdBytes = new byte[16];
			private readonly byte[] signBytes = new byte[sizeof(char)];

			[CLSCompliant(false)]
			public Reader(CounterStorage parent, StorageEnvironment storageEnvironment)
			{
				this.parent = parent;
				transaction = storageEnvironment.NewTransaction(TransactionFlags.Read);
				serverNamesToIds = transaction.State.GetTree(transaction, "serverNames->Ids");
				serverIdsToNames = transaction.State.GetTree(transaction, "Ids->serverNames");
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

            public long GetServersCount()
            {
                return serverNamesToIds.State.EntriesCount;
            }

			public IEnumerable<string> GetCounterNames(string prefix)
			{
				using (var it = countersToEtags.Iterate())
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
				using (var it = counters.Iterate())
				{
					it.RequiredPrefix = fullCounterName;
					if (it.Seek(fullCounterName) == false)
						return null;

					//full counter name example: foo/bar/000-guid-000/+
					var serverId = fullCounterName.Substring(fullCounterName.Length - 17, 16);
					var serverIdGuid = new Guid(serverId);
					var sign = fullCounterName.Substring(fullCounterName.Length - 1, 1);
					return new CounterValue
					{
						ServerId = serverIdGuid,
						ServerName = ServerNameFor(serverIdGuid),
						Value = it.CreateReaderForCurrent().ReadBigEndianInt64(),
						IsPositive = sign == ValueSign.Positive
					};
				}
			}

			public Counter GetCountersByPrefix(string groupName, string counterName)
			{
				var fullCounterName = FullCounterName(groupName, counterName);
				return GetCountersByPrefix(fullCounterName);
			}

			public Counter GetCountersByPrefix(string namePrefix)
			{
	            using (var it = counters.Iterate())
				{
					it.RequiredPrefix = namePrefix;
					if (it.Seek(namePrefix) == false)
						return null;

					var result = new Counter();
					do
					{
						//TODO: better extract
						var x = it.CurrentKey.ToString();
						it.CurrentKey.CopyTo(it.CurrentKey.Size - 17, serverIdBytes, 0, 16);
						/*var serverId = EndianBitConverter.ToString(serverIdBytes);
						var serverIdGuid = new Guid(serverId);*/
						var serverIdGuid = new Guid(x.Substring(x.Length - 38, 36));
						it.CurrentKey.CopyTo(it.CurrentKey.Size - 1, signBytes, 0, 1);
						//var sign = EndianBitConverter.ToString(signBytes);//EndianBitConverter.Big.ToChar(signBytes, 1);
						var sign = x.Substring(x.Length - 2, 1);
						result.CounterValues.Add(new CounterValue
						{
							ServerId = serverIdGuid,
							ServerName = ServerNameFor(serverIdGuid),
							Value = it.CreateReaderForCurrent().ReadLittleEndianInt64(),
							IsPositive = sign == ValueSign.Positive
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

			public string ServerNameFor(Guid serverId)
			{
				string serverName;
				parent.ServerNamesToIds.TryGetValue(serverId.ToString(), out serverName);
				return serverName;
			}

			public string SourceIdFor(string serverName)
			{
				string sourceServerId;
				parent.ServerNamesToIds.TryGetValue(serverName, out sourceServerId);
				return sourceServerId;
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
			private readonly Tree serverNamesToIds, serverIdsToNames, serversLastEtag, counters, etagsToCounters, countersToEtag, countersGroups, metadata;
            private readonly byte[] tempThrowawayBuffer;
			private byte[] counterFullNameInBytes = new byte[0];
			private readonly byte[] etagBuffer = new byte[sizeof(long)];
		    //private readonly Reader reader;
			private readonly int storeBufferLength;

			public Writer(CounterStorage parent, StorageEnvironment storageEnvironment)
			{
				this.parent = parent;
				transaction = storageEnvironment.NewTransaction(TransactionFlags.Read);
				writeBatch = new WriteBatch();
				snapshotReader = new SnapshotReader(transaction);
                //reader = new Reader(transaction);
				serverNamesToIds = transaction.State.GetTree(transaction, "serverNames->Ids");
				serverIdsToNames = transaction.State.GetTree(transaction, "Ids->serverNames");
				serversLastEtag = transaction.State.GetTree(transaction, "servers->lastEtag");
                counters = transaction.State.GetTree(transaction, "counters");
				countersGroups = transaction.State.GetTree(transaction, "countersGroups");
				etagsToCounters = transaction.State.GetTree(transaction, "etags->counters");
				countersToEtag = transaction.State.GetTree(transaction, "counters->etags");
				metadata = transaction.State.GetTree(transaction, "$metadata");

				tempThrowawayBuffer = new byte[sizeof (long)];

				storeBufferLength = tempThrowawayBuffer.Length;
			}

			public CounterValue GetCounterValue(string fullCounterName)
			{
				return parent.CreateReader().GetCounterValue(fullCounterName);
			}

			private Counter GetCountersByPrefix(string name)
            {
                return parent.CreateReader().GetCountersByPrefix(name);
            }

			public long GetLastEtagFor(string serverName)
			{
				return parent.CreateReader().GetLastEtagFor(serverName);
			}

			public string ServerIdFor(string serverName)
			{
				return parent.CreateReader().SourceIdFor(serverName);
			}

			/*public void Store(string serverName, string fullCounterName, long delta)
		    {
				Store(serverName, fullCounterName, delta);
				/*Store(serverName, fullCounterName, result =>
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
		        });#1#
		    }*/

			//full counter name: foo/bar/

			public void Store(string groupName, string counterName, long delta)
			{
				var fullCounterName = FullCounterName(groupName, counterName);
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
				UpdateServers(serverId, counterValue.ServerName);
				Store(serverId, fullCounterName, sign, counterKey =>
				{
					EndianBitConverter.Big.CopyBytes(counterValue.Value, tempThrowawayBuffer, 0);
					var valueSlice = new Slice(tempThrowawayBuffer);
					writeBatch.Add(counterKey, valueSlice, counters.Name, shouldIgnoreConcurrencyExceptions: true);
				});
            }

			//full counter name: foo/bar/
			private void Store(string serverId, string fullCounterName, string sign, Action<Slice> storeAction)
			{
				var fullCounterNameSize = Encoding.UTF8.GetByteCount(fullCounterName);
				var requiredBufferSize = fullCounterNameSize + 16;
				EnsureBufferSize(requiredBufferSize);
				Debug.Assert(requiredBufferSize < UInt16.MaxValue);

				var sliceWriter = new SliceWriter(counterFullNameInBytes);
				sliceWriter.WriteString(fullCounterName);
				sliceWriter.WriteString(serverId);
				sliceWriter.WriteString(sign);

				//TODO: verify this is upper level
				var endOfGroupNameIndex = Array.IndexOf(counterFullNameInBytes, Constants.CountersSeperatorByte, 0, fullCounterNameSize);
				if (endOfGroupNameIndex == -1)
					throw new InvalidOperationException("Could not find group name in counter, no separator");

				var counterKey = sliceWriter.CreateSlice();
				var readResult = snapshotReader.Read(counters.Name, counterKey, writeBatch); //= counters.Read(counterKey);
				if (readResult == null && !IsCounterExists(fullCounterName)) //it's a new counter
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

				//save counter full name and its value into the counters tree
				storeAction(counterKey);

				counterKey = new Slice(counterFullNameInBytes, (ushort) fullCounterNameSize);
				readResult = snapshotReader.Read(countersToEtag.Name, counterKey, writeBatch);
				
				if (readResult != null) // remove old etag entry
				{
					readResult.Reader.Read(etagBuffer, 0, sizeof (long));
                    var oldEtagSlice = new Slice(etagBuffer);
                    //etagsToCounters.Delete(oldEtagSlice);
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
				var fullCounterName = FullCounterName(groupName, counterName);
				var countersByPrefix = GetCountersByPrefix(fullCounterName);
				if (countersByPrefix == null)
					return false;

				var overallTotalPositive = countersByPrefix.CounterValues.Where(x => x.IsPositive).Sum(x => x.Value);
				var overallTotalNegative = countersByPrefix.CounterValues.Where(x => !x.IsPositive).Sum(x => x.Value);
				var difference = overallTotalPositive - overallTotalNegative;

				if (difference != 0)
				{
					difference = -difference;
					Store(parent.ServerId, fullCounterName, difference);
					parent.MetricsCounters.Resets.Mark();
					return true;
				}
				return false;
			}

			public void RecordLastEtagFor(string serverId, string serverName, long lastEtag)
			{
				UpdateServers(serverId, serverName);
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



			private void UpdateServers(string serverId, string serverName)
			{
				parent.ServerNamesToIds.GetOrAdd(serverName, arg => serverId);
				parent.ServerIdsToNames.GetOrAdd(serverId, arg => serverName);

				var result = snapshotReader.Read(serverNamesToIds.Name, serverName, writeBatch);
				if (result == null)
				{
					writeBatch.Add(serverName, serverId, serverNamesToIds.Name, shouldIgnoreConcurrencyExceptions: true);
				}

				result = snapshotReader.Read(serverIdsToNames.Name, serverId, writeBatch);
				if (result == null)
				{
					writeBatch.Add(serverId, serverName, serverIdsToNames.Name, shouldIgnoreConcurrencyExceptions: true);
				}

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
				parent.storageEnvironment.Writer.Write(writeBatch);
				if (notifyParent)
				{
					parent.Notify();
				}
			}

			public void Dispose()
			{
				//writeBatch.Dispose();
				//snapshotReader.Dispose();
				parent.LastWrite = SystemTime.UtcNow;
                if (transaction != null)
					transaction.Dispose();
			}
		}

		private static string FullCounterName(string groupName, string counterName)
		{
			return String.Format("{0}{1}{2}{3}", groupName, Constants.CountersSeperatorString, counterName, Constants.CountersSeperatorString);
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