using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Raven.Abstractions;
using Raven.Abstractions.Replication;
using Raven.Database.Config;
using Raven.Database.Counters.Controllers;
using Voron;
using Voron.Impl;
using Voron.Trees;
using Voron.Util;
using Voron.Util.Conversion;
using Constants = Raven.Abstractions.Data.Constants;
using JsonSerializer = Newtonsoft.Json.JsonSerializer;

namespace Raven.Database.Counters
{
	public class CounterStorage : IDisposable
	{
        public string CounterStorageUrl { get; private set; }
        private readonly StorageEnvironment storageEnvironment;
        public readonly RavenCounterReplication ReplicationTask;
        public Guid Id { get; private set; }
		public DateTime LastWrite { get; private set; }

	    public long LastEtag { get; private set; }

        public event Action CounterUpdated = () => { };

        public int ReplicationTimeoutInMs { get; private set; }

	    public readonly string CounterStorageName;

		public CounterStorage(string serverUrl, string counterStorageStorageName, InMemoryRavenConfiguration configuration)
		{
            CounterStorageUrl = String.Format("{0}counters/{1}", serverUrl, counterStorageStorageName);
            CounterStorageName = counterStorageStorageName;
                
			var options = configuration.RunInMemory ? StorageEnvironmentOptions.CreateMemoryOnly()
				: CreateStorageOptionsFromConfiguration(configuration.CountersDataDirectory, configuration.Settings);

			storageEnvironment = new StorageEnvironment(options);
            ReplicationTask = new RavenCounterReplication(this);
		   
		    ReplicationTimeoutInMs = configuration.GetConfigurationValue<int>("Raven/Replication/ReplicationRequestTimeout") ?? 60*1000;

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
				var id = metadata.Read(tx, "id");

				if (id == null) // new counter db
				{
					var serverIdBytes = EndianBitConverter.Big.GetBytes(0); // local is always 0
					var serverIdSlice = new Slice(serverIdBytes);
					serverNamesToIds.Add(tx, CounterStorageUrl, serverIdSlice);
					serverIdsToNames.Add(tx, serverIdSlice, CounterStorageUrl);

					Id = Guid.NewGuid();
					metadata.Add(tx, "id", Id.ToByteArray());
					metadata.Add(tx, "name", Encoding.UTF8.GetBytes(CounterStorageUrl));

					//TODO: Remove this when UI is finished
						ReplicationDestination replication1 = new ReplicationDestination();
						ReplicationDestination replication2 = new ReplicationDestination();
						if (CounterStorageUrl.Contains(":8080"))
						{
							replication1.Url = CounterStorageUrl.Replace(":8080", ":8081");
							replication2.Url = CounterStorageUrl.Replace(":8080", ":8082");
						}
						else if (CounterStorageUrl.Contains(":8081"))
						{
							replication1.Url = CounterStorageUrl.Replace(":8081", ":8080");
							replication2.Url = CounterStorageUrl.Replace(":8081", ":8082");
						}
						else
						{
							replication1.Url = CounterStorageUrl.Replace(":8082", ":8080");
							replication2.Url = CounterStorageUrl.Replace(":8082", ":8081");
						}

						replication1.Disabled = false;
						replication2.Disabled = false;

						ReplicationDocument document = new ReplicationDocument();
						document.Destinations.Add(replication1);
						document.Destinations.Add(replication2);
						
						using (var memoryStream = new MemoryStream())
						using (var streamWriter = new StreamWriter(memoryStream))
						using (var jsonTextWriter = new JsonTextWriter(streamWriter))
						{
							new JsonSerializer().Serialize(jsonTextWriter, document);
							streamWriter.Flush();
							memoryStream.Position = 0;
							metadata.Add(tx, "replication", memoryStream);
						}
					//TODO: Remove this when UI is finished

					tx.Commit();
				}
				else // existing counter db
				{
					int used;
					Id = new Guid(id.Reader.ReadBytes(16, out used));
					var nameResult = metadata.Read(tx, "name");
					if (nameResult == null)
						throw new InvalidOperationException("Could not read name from the store, something bad happened");
					var storedName = new StreamReader(nameResult.Reader.AsStream()).ReadToEnd();

					if (storedName != CounterStorageUrl)
						throw new InvalidOperationException("The stored name " + storedName + " does not match the given name " + CounterStorageUrl);

					using (var it = etags.Iterate(tx))
					{
						if (it.Seek(Slice.AfterAllKeys))
						{
							LastEtag = it.CurrentKey.CreateReader().ReadBigEndianInt64();
						}
					}
				}

                ReplicationTask.StartReplication();
			}
		}

		private static StorageEnvironmentOptions CreateStorageOptionsFromConfiguration(string path, NameValueCollection settings)
		{
			bool allowIncrementalBackupsSetting;
			if (bool.TryParse(settings["Raven/Voron/AllowIncrementalBackups"] ?? "false", out allowIncrementalBackupsSetting) == false)
				throw new ArgumentException("Raven/Voron/AllowIncrementalBackups settings key contains invalid value");

			var directoryPath = path ?? AppDomain.CurrentDomain.BaseDirectory;
			var filePathFolder = new DirectoryInfo(directoryPath);
			if (filePathFolder.Exists == false)
				filePathFolder.Create();

			var tempPath = settings["Raven/Voron/TempPath"];
			var journalPath = settings[Constants.RavenTxJournalPath];
			var options = StorageEnvironmentOptions.ForPath(directoryPath, tempPath, journalPath);
			options.IncrementalBackupEnabled = allowIncrementalBackupsSetting;
			return options;
		}

		public Reader CreateReader()
		{
			return new Reader(this, storageEnvironment);
		}

		public Writer CreateWriter()
		{
			
			LastWrite = SystemTime.UtcNow;
			return new Writer(this, storageEnvironment);
		}

	    public void Notify()
	    {
	        CounterUpdated();
	    }

		public void Dispose()
		{
            ReplicationTask.Dispose();
			if (storageEnvironment != null)
				storageEnvironment.Dispose();
		}

		public class Reader : IDisposable
		{
		    private readonly CounterStorage parent;
		    private readonly Transaction transaction;
			private readonly Tree serverNamesToIds, serverIdsToNames, serversLastEtag, counters, countersEtags, countersGroups, etagsCounters, metadata;
			private readonly byte[] serverIdBytes = new byte[sizeof(int)];

            public Reader(CounterStorage parent, StorageEnvironment storageEnvironment)
                : this(parent, storageEnvironment.NewTransaction(TransactionFlags.Read)) { }

            public Reader(CounterStorage parent, Transaction t)
            {
                this.parent = parent;
                transaction = t;
				serverNamesToIds = transaction.State.GetTree(transaction, "serverNames->Ids");
				serverIdsToNames = transaction.State.GetTree(transaction, "Ids->serverNames");
				serversLastEtag = transaction.State.GetTree(transaction, "servers->lastEtag");
                counters = transaction.State.GetTree(transaction, "counters");
                countersGroups = transaction.State.GetTree(transaction, "countersGroups");
                countersEtags = transaction.State.GetTree(transaction, "counters->etags");
                etagsCounters = transaction.State.GetTree(transaction, "etags->counters");
				metadata = transaction.State.GetTree(transaction, "$metadata");
            }

			public IEnumerable<string> GetCounterNames(string prefix)
			{
				using (var it = countersEtags.Iterate(transaction))
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

			public IEnumerable<string> GetCounterGroups()
			{
				using (var it = countersGroups.Iterate(transaction))
				{
					if (it.Seek(Slice.BeforeAllKeys) == false)
						yield break;
					do
					{
						yield return it.CurrentKey.ToString();
					} while (it.MoveNext());
				}
			}

            public Counter GetCounter(Slice name)
			{
				Slice slice = name;
				var etagResult = countersEtags.Read(transaction, slice);
				if (etagResult == null)
					return null;
                var etag = etagResult.Reader.ReadBigEndianInt64();
				using (var it = counters.Iterate(transaction))
				{
					it.RequiredPrefix = slice;
					if (it.Seek(slice) == false)
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
                
                using (var it = etagsCounters.Iterate(transaction))
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

                        var counter = GetCounter(counterName);
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
                using (var it = serversLastEtag.Iterate(transaction))
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
				int serverId = -1;
				var key = Encoding.UTF8.GetBytes(server);
				var result = serverNamesToIds.Read(transaction, new Slice(key));

				if (result != null && result.Version != 0)
				{
					serverId = result.Reader.ReadBigEndianInt32();
				}

				return serverId;
			}

			public long GetLastEtagFor(string server)
			{
				long serverEtag = 0;
				int serverId = GetServerId(server);
				if (serverId == -1)
				{
					return serverEtag;
				}

				var key = EndianBitConverter.Big.GetBytes(serverId);
				var result = serversLastEtag.Read(transaction, new Slice(key));
				if (result != null && result.Version != 0)
				{
					serverEtag = result.Reader.ReadBigEndianInt64();
				}

				return serverEtag;
			}

			public ReplicationDocument GetReplicationData()
			{
				var readResult = metadata.Read(transaction, "replication");
				if (readResult != null)
				{
					var stream = readResult.Reader.AsStream();
					stream.Position = 0;
					using (var streamReader = new StreamReader(stream))
					using (var jsonTextReader = new JsonTextReader(streamReader))
					{
						return new JsonSerializer().Deserialize<ReplicationDocument>(jsonTextReader);
					}
				}
				return null;
			}

			public string ServerNameFor(int serverId)
			{
				string serverName = string.Empty;

				var key = EndianBitConverter.Big.GetBytes(serverId);
				var result = serverIdsToNames.Read(transaction, new Slice(key));

				if (result != null && result.Version != 0)
				{
					serverName = result.Reader.AsSlice().ToString();
				}

				return serverName;
			}

			public int SourceIdFor(string serverName)
			{
				int serverId = 0;
				var key = Encoding.UTF8.GetBytes(serverName);
				var result = serverNamesToIds.Read(transaction, new Slice(key));

				if (result != null && result.Version != 0)
				{
					serverId = result.Reader.ReadBigEndianInt32();
				}

				return serverId;
			}

            public void Dispose()
			{
				if (transaction != null)
					transaction.Dispose();
			}
		}

		public class Writer : IDisposable
		{
			private readonly CounterStorage parent;
			private readonly Transaction transaction;
			private readonly Tree serverNamesToIds, serverIdsToNames, serversLastEtag, counters, etagsCountersIx, countersEtagIx, countersGroups, metadata;
            private readonly byte[] storeBuffer;
			private byte[] buffer = new byte[0];
			private readonly byte[] etagBuffer = new byte[sizeof(long)];
		    private readonly Reader reader;

			public Writer(CounterStorage parent, StorageEnvironment storageEnvironment)
			{
				this.parent = parent;
                transaction = storageEnvironment.NewTransaction(TransactionFlags.ReadWrite);
                reader = new Reader(parent, transaction);
				serverNamesToIds = transaction.State.GetTree(transaction, "serverNames->Ids");
				serverIdsToNames = transaction.State.GetTree(transaction, "Ids->serverNames");
				serversLastEtag = transaction.State.GetTree(transaction, "servers->lastEtag");
                counters = transaction.State.GetTree(transaction, "counters");
				countersGroups = transaction.State.GetTree(transaction, "countersGroups");
				etagsCountersIx = transaction.State.GetTree(transaction, "etags->counters");
				countersEtagIx = transaction.State.GetTree(transaction, "counters->etags");
				metadata = transaction.State.GetTree(transaction, "$metadata");

				storeBuffer = new byte[sizeof(long) + //positive
									   sizeof(long)]; // negative
			}

            public Counter GetCounter(string name)
            {
                return reader.GetCounter(name);
            }

			public long GetLastEtagFor(string server)
			{
				return reader.GetLastEtagFor(server);
			}

			public int SourceIdFor(string serverName)
			{
				return reader.SourceIdFor(serverName);
			}

		    public void Store(string server, string counter, long delta)
		    {
		        Store(server, counter, result =>
		        {
		            int valPos = 0;
		            if (delta < 0)
		            {
		                valPos = 8;
		                delta = -delta;
		            }

		            if (result == null)
		            {
		                EndianBitConverter.Big.CopyBytes(delta, storeBuffer, valPos);
		                EndianBitConverter.Big.CopyBytes(0L, storeBuffer, valPos == 0 ? 8 : 0);
		            }
		            else
		            {
		                result.Reader.Read(storeBuffer, 0, buffer.Length);
		                delta += EndianBitConverter.Big.ToInt64(storeBuffer, valPos);
		                EndianBitConverter.Big.CopyBytes(delta, storeBuffer, valPos);
		            }
		        });
		    }

            public void Store(string server, string counter, long positive, long negative)
            {
                Store(server, counter, result =>
                {
                    EndianBitConverter.Big.CopyBytes(positive, storeBuffer, 0);
                    EndianBitConverter.Big.CopyBytes(negative, storeBuffer, 8);
                });
            }

			private void Store(string server, string counter, Action<ReadResult> setStoreBuffer)
			{
				
                parent.LastEtag++;
				var serverId = GetOrAddServerId(server);

				var counterNameSize = Encoding.UTF8.GetByteCount(counter);
				var requiredBufferSize = counterNameSize + sizeof (int);
				EnsureBufferSize(requiredBufferSize);

				var end = Encoding.UTF8.GetBytes(counter, 0, counter.Length, buffer, 0);
				EndianBitConverter.Big.CopyBytes(serverId, buffer, end);

				var endOfGroupPrefix = Array.IndexOf(buffer, (byte) ';', 0, counterNameSize);
				if (endOfGroupPrefix == -1)
					throw new InvalidOperationException("Could not find group name in counter, no ; separator");

				var groupKeySlice = new Slice(buffer, (ushort) endOfGroupPrefix);
				bool isGroupExists = countersGroups.Read(transaction, groupKeySlice) != null;
				if (!isGroupExists)
				{
					countersGroups.Add(transaction, groupKeySlice, EndianBitConverter.Little.GetBytes(1));
				}

				Debug.Assert(requiredBufferSize < ushort.MaxValue);
				var slice = new Slice(buffer, (ushort) requiredBufferSize);
				var result = counters.Read(transaction, slice);

				if (isGroupExists && result == null)
				{
					countersGroups.Increment(transaction, groupKeySlice, 1);
				}

				setStoreBuffer(result);

				counters.Add(transaction, slice, storeBuffer);

				slice = new Slice(buffer, (ushort) counterNameSize);
				result = countersEtagIx.Read(transaction, slice);
				
                
				if (result != null) // remove old etag entry
				{
					result.Reader.Read(etagBuffer, 0, sizeof (long));
                    var oldEtagSlice = new Slice(etagBuffer);
                    etagsCountersIx.Delete(transaction, oldEtagSlice);
				}
                
				EndianBitConverter.Big.CopyBytes(parent.LastEtag, etagBuffer, 0);
                var newEtagSlice = new Slice(etagBuffer);
                etagsCountersIx.Add(transaction, newEtagSlice, slice);
                countersEtagIx.Add(transaction, slice, newEtagSlice);
			}

			private void EnsureBufferSize(int requiredBufferSize)
			{
				if (buffer.Length < requiredBufferSize)
					buffer = new byte[Utils.NearestPowerOfTwo(requiredBufferSize)];
			}

			private int GetOrAddServerId(string server)
			{
				int serverId = 0;
				var result = serverNamesToIds.Read(transaction, server);

				if (result != null && result.Version != 0)
				{
					serverId = result.Reader.ReadBigEndianInt32();
				}
				else
				{
					serverId = (int)serverNamesToIds.State.EntriesCount; //todo: should we check for overflow?
					var serverIdBytes = EndianBitConverter.Big.GetBytes(serverId);
					var serverIdSlice = new Slice(serverIdBytes);
					serverNamesToIds.Add(transaction, server, serverIdSlice);
					serverIdsToNames.Add(transaction, serverIdSlice, server);
				}

				return serverId;
			}

			public void RecordLastEtagFor(string server, long lastEtag)
			{
				var serverId = GetOrAddServerId(server);
				var key = EndianBitConverter.Big.GetBytes(serverId);
				serversLastEtag.Add(transaction, new Slice(key), EndianBitConverter.Big.GetBytes(lastEtag));
			}

			public void UpdateReplications(ReplicationDocument document)
			{
				using (var memoryStream = new MemoryStream())
				using (var streamWriter = new StreamWriter(memoryStream))
				using (var jsonTextWriter = new JsonTextWriter(streamWriter))
				{
					new JsonSerializer().Serialize(jsonTextWriter, document);
					streamWriter.Flush();
					memoryStream.Position = 0;
					metadata.Add(transaction, "replication", memoryStream);
				}

				parent.ReplicationTask.SignalCounterUpdate();
			}

			public void Commit()
			{
				transaction.Commit();
                parent.Notify();
			}

			public void Dispose()
			{
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
	}
}