using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Raven.Abstractions;
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

		private const int ServerId = 0; // local is always 0

		public CounterStorage(string serverUrl, string counterStorageStorageName, InMemoryRavenConfiguration configuration)
		{
            CounterStorageUrl = String.Format("{0}counters/{1}", serverUrl, counterStorageStorageName);
            CounterStorageName = counterStorageStorageName;
                
			var options = configuration.RunInMemory ? StorageEnvironmentOptions.CreateMemoryOnly()
				: CreateStorageOptionsFromConfiguration(configuration.CountersDataDirectory, configuration.Settings);

			storageEnvironment = new StorageEnvironment(options);
            ReplicationTask = new RavenCounterReplication(this);

			//TODO: add an option to create a ReplicationRequestTimeout when creating a new counter storage
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
				var id = metadata.Read("id");

				if (id == null) // new counter db
				{
					var serverIdBytes = EndianBitConverter.Big.GetBytes(ServerId); 
					var serverIdSlice = new Slice(serverIdBytes);
					serverNamesToIds.Add(CounterStorageUrl, serverIdSlice);
					serverIdsToNames.Add(serverIdSlice, CounterStorageUrl);

					Id = Guid.NewGuid();
					metadata.Add("id", Id.ToByteArray());
					metadata.Add("name", Encoding.UTF8.GetBytes(CounterStorageName));

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

					if (storedName != CounterStorageName)
						throw new InvalidOperationException("The stored name " + storedName + " does not match the given name " + CounterStorageName);

					using (var it = etags.Iterate())
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

	    private void Notify()
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
							NumOfCounters = it.CreateReaderForCurrent().ReadLittleEndianInt64()
						};
					} while (it.MoveNext());
				}
			}

            public Counter GetCounter(Slice name)
			{
				Slice slice = name;
				var etagResult = countersEtags.Read(slice);
				if (etagResult == null)
					return null;
                var etag = etagResult.Reader.ReadBigEndianInt64();
				using (var it = counters.Iterate())
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
				int serverId = -1;
				var key = Encoding.UTF8.GetBytes(server);
				var result = serverNamesToIds.Read(new Slice(key));

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
				var result = serversLastEtag.Read(new Slice(key));
				if (result != null && result.Version != 0)
				{
					serverEtag = result.Reader.ReadBigEndianInt64();
				}

				return serverEtag;
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
				string serverName = string.Empty;

				var key = EndianBitConverter.Big.GetBytes(serverId);
				var result = serverIdsToNames.Read(new Slice(key));

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
				var result = serverNamesToIds.Read(new Slice(key));

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
						result.Reader.Read(storeBuffer, 0, storeBuffer.Length);
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

			public bool Reset(string server, string fullCounterName)
			{
				Counter counter = GetCounter(fullCounterName); //TODO: implement get counter without an etag
				if (counter != null)
				{
					long overallTotalPositive = counter.ServerValues.Sum(x => x.Positive);
					long overallTotalNegative = counter.ServerValues.Sum(x => x.Negative);
					long difference = overallTotalPositive - overallTotalNegative;

					if (difference != 0)
					{
						difference = -difference;
						Store(server, fullCounterName, difference);
						return true;
					}
				}
				return false;
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

				var endOfGroupPrefix = Array.IndexOf(buffer, Constants.GroupSeperator, 0, counterNameSize);
				if (endOfGroupPrefix == -1)
					throw new InvalidOperationException("Could not find group name in counter, no separator");

				var groupKeySlice = new Slice(buffer, (ushort) endOfGroupPrefix);

				Debug.Assert(requiredBufferSize < ushort.MaxValue);
				var slice = new Slice(buffer, (ushort) requiredBufferSize);
				var result = counters.Read(slice);

				if (result == null && !IsCounterAlreadyExists(counter)) //if it's a new counter
				{
					countersGroups.Increment(groupKeySlice, 1);
				}

				setStoreBuffer(result);

				counters.Add(slice, storeBuffer);

				slice = new Slice(buffer, (ushort) counterNameSize);
				result = countersEtagIx.Read(slice);
				
				if (result != null) // remove old etag entry
				{
					result.Reader.Read(etagBuffer, 0, sizeof (long));
                    var oldEtagSlice = new Slice(etagBuffer);
                    etagsCountersIx.Delete(oldEtagSlice);
				}
                
				EndianBitConverter.Big.CopyBytes(parent.LastEtag, etagBuffer, 0);
                var newEtagSlice = new Slice(etagBuffer);
                etagsCountersIx.Add(newEtagSlice, slice);
                countersEtagIx.Add(slice, newEtagSlice);
			}

			public void RecordLastEtagFor(string server, long lastEtag)
			{
				var serverId = GetOrAddServerId(server);
				var key = EndianBitConverter.Big.GetBytes(serverId);
				serversLastEtag.Add(new Slice(key), EndianBitConverter.Big.GetBytes(lastEtag));
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
				if (buffer.Length < requiredBufferSize)
					buffer = new byte[Utils.NearestPowerOfTwo(requiredBufferSize)];
			}

			private int GetOrAddServerId(string server)
			{
				int serverId;
				var result = serverNamesToIds.Read(server);

				if (result != null && result.Version != 0)
				{
					serverId = result.Reader.ReadBigEndianInt32();
				}
				else
				{
					serverId = (int)serverNamesToIds.State.EntriesCount; //todo: should we check for overflow or change the server id to long?
					var serverIdBytes = EndianBitConverter.Big.GetBytes(serverId);
					var serverIdSlice = new Slice(serverIdBytes);
					serverNamesToIds.Add(server, serverIdSlice);
					serverIdsToNames.Add(serverIdSlice, server);
				}

				return serverId;
			}

			private bool IsCounterAlreadyExists(Slice name)
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

	    public class ServerEtag
	    {
	        public int SourceId { get; set; }
	        public long Etag { get; set; }
	    }
	}
}