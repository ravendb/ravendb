using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Raven.Abstractions;
using Raven.Database.Config;
using Raven.Database.Counters.Controllers;
using Voron;
using Voron.Impl;
using Voron.Trees;
using Voron.Util;
using Voron.Util.Conversion;
using Constants = Raven.Abstractions.Data.Constants;

namespace Raven.Database.Counters
{
	public class CounterStorage : IDisposable
	{
	    public string Name { get; private set; }
        private readonly StorageEnvironment storageEnvironment;
        private readonly RavenCounterReplication replication;
        public Guid Id { get; private set; }
		public DateTime LastWrite { get; private set; }

		private Dictionary<string, int> serverNamesToIds = new Dictionary<string, int>();
		private Dictionary<int, string> serverIdstoName = new Dictionary<int, string>();
	    
	    public long LastEtag { get; private set; }

        public event Action CounterUpdated = () => { };

		public CounterStorage(string name, InMemoryRavenConfiguration configuration)
		{
		    Name = name;
			var options = configuration.RunInMemory ? StorageEnvironmentOptions.CreateMemoryOnly()
				: CreateStorageOptionsFromConfiguration(configuration.CountersDataDirectory, configuration.Settings);

			storageEnvironment = new StorageEnvironment(options);
            replication = new RavenCounterReplication(this);
			Initialize(name);
		}

		private void Initialize(string name)
		{
			using (var tx = storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
			{
				storageEnvironment.CreateTree(tx, "serversLastEtag");
				storageEnvironment.CreateTree(tx, "counters");
				storageEnvironment.CreateTree(tx, "countersGroups");

				var servers = storageEnvironment.CreateTree(tx, "servers");
				var etags = storageEnvironment.CreateTree(tx, "etags->counters");
				storageEnvironment.CreateTree(tx, "counters->etags");
				var metadata = tx.State.GetTree(tx, "$metadata");
				var id = metadata.Read(tx, "id");
				if (id == null) // new db
				{
					Id = Guid.NewGuid();
					// local is always 0
					servers.Add(tx, name, BitConverter.GetBytes(0));
					serverNamesToIds[name] = 0;
					serverIdstoName[0] = name;
					metadata.Add(tx, "id", Id.ToByteArray());
					metadata.Add(tx, "name", Encoding.UTF8.GetBytes(name));

                    //HACK: setup replication peer
				    if (name.Contains(":8080"))
				    {
				        name = name.Replace(":8080", ":8081");
				    }
				    else
				    {
                        name = name.Replace(":8081", ":8080");
				    }

				    servers.Add(tx, name, BitConverter.GetBytes(1));
				    serverNamesToIds[name] = 1;
				    serverIdstoName[1] = name;
				    

                    tx.Commit();
				}
				else // existing db
				{
					Id = new Guid(id.Reader.ReadBytes(16));
					var nameResult = metadata.Read(tx, "name");
					if (nameResult == null)
						throw new InvalidOperationException("Could not read name from the store, something bad happened");
					var storedName = new StreamReader(nameResult.Reader.AsStream()).ReadToEnd();

					if (storedName != name)
						throw new InvalidOperationException("The stored name " + storedName + " does not match the given name " + name);


					using (var it = servers.Iterate(tx))
					{
						if (it.Seek(Slice.BeforeAllKeys))
						{
							do
							{
								var serverId = it.CreateReaderForCurrent().ReadInt32();
								var serverName = it.CurrentKey.ToString();
								serverNamesToIds[serverName] = serverId;
								serverIdstoName[serverId] = serverName;
							} while (it.MoveNext());
						}
					}
					using (var it = etags.Iterate(tx))
					{
						if (it.Seek(Slice.AfterAllKeys))
						{
							LastEtag = it.CurrentKey.ToInt64();
						}
					}
				}
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
			if (storageEnvironment != null)
				storageEnvironment.Dispose();

            replication.ShutDown(); //TODO: make this IDisposable instead?
		}

		public class Reader : IDisposable
		{
		    private readonly CounterStorage parent;
		    private readonly Transaction transaction;
            private readonly Tree counters, countersEtags, countersGroups, etagsCounters;
			private readonly byte[] serverIdBytes = new byte[sizeof(int)];

            public Reader(CounterStorage parent, StorageEnvironment storageEnvironment)
                : this(parent, storageEnvironment.NewTransaction(TransactionFlags.Read)) { }

            public Reader(CounterStorage parent, Transaction t)
            {
                this.parent = parent;
                transaction = t;
                counters = transaction.State.GetTree(transaction, "counters");
                countersGroups = transaction.State.GetTree(transaction, "countersGroups");
                countersEtags = transaction.State.GetTree(transaction, "counters->etags");
                etagsCounters = transaction.State.GetTree(transaction, "etags->counters");
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
				var etag = etagResult.Reader.ReadInt64();
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
							Positive = reader.ReadInt64(),
							Negative = reader.ReadInt64()
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
	                    if (buffer.Length < it.GetCurrentDataSize())
	                    {
							buffer = new byte[Utils.NearestPowerOfTwo(it.GetCurrentDataSize())];
	                    }
	                    it.CreateReaderForCurrent().Read(buffer, 0, buffer.Length);
                        var counterName = Encoding.UTF8.GetString(buffer);
                        var counter = GetCounter(counterName);
                        yield return new ReplicationCounter
                        {
                            CounterName = counterName,
                            Etag = counter.Etag,
                            ServerValues = counter.ServerValues.Select(x => new ReplicationCounter.PerServerValue
                            {
                                ServerName = parent.ServerNameFor(x.SourceId),
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
                var serversLastEtag = transaction.State.GetTree(transaction, "serversLastEtag");
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
			private readonly Tree counters, etagsCountersIx, countersEtagIx, countersGroups;
            private readonly byte[] storeBuffer;
			private byte[] buffer = new byte[0];
			private readonly byte[] etagBuffer = new byte[sizeof(long)];
		    private readonly Reader reader;

			public Writer(CounterStorage parent, StorageEnvironment storageEnvironment)
			{
				this.parent = parent;
                transaction = storageEnvironment.NewTransaction(TransactionFlags.ReadWrite);
                reader = new Reader(parent, transaction);
                counters = transaction.State.GetTree(transaction, "counters");
				countersGroups = transaction.State.GetTree(transaction, "countersGroups");
				etagsCountersIx = transaction.State.GetTree(transaction, "etags->counters");
				countersEtagIx = transaction.State.GetTree(transaction, "counters->etags");

				storeBuffer = new byte[sizeof(long) + //positive
									   sizeof(long)]; // negative
			}

            public Counter GetCounter(string name)
            {
                return reader.GetCounter(name);
            }

		    public void Store(string server, string counter, long delta)
		    {
		        Store(server, counter, result =>
		        {
		            var valPos = delta > 0 ? 0 : 8;
		            if (result == null)
		            {
		                EndianBitConverter.Big.CopyBytes(delta, storeBuffer, valPos);
		                EndianBitConverter.Big.CopyBytes(0L, storeBuffer, delta > 0 ? 8 : 0);
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
				var serverId = GetServerId(server);

				var counterNameSize = Encoding.UTF8.GetByteCount(counter);
				var requiredBufferSize = counterNameSize + sizeof (int);
				EnsureBufferSize(requiredBufferSize);

				var end = Encoding.UTF8.GetBytes(counter, 0, counter.Length, buffer, 0);
				EndianBitConverter.Big.CopyBytes(serverId, buffer, end);

				var endOfGroupPrefix = Array.IndexOf(buffer, (byte) ';', 0, counterNameSize);
				if (endOfGroupPrefix == -1)
					throw new InvalidOperationException("Could not find group name in counter, no ; separator");

				var groupKeySlice = new Slice(buffer, (ushort) endOfGroupPrefix);
				if (countersGroups.Read(transaction, groupKeySlice) == null)
				{
					countersGroups.Add(transaction, groupKeySlice, new byte[0]);
				}

				Debug.Assert(requiredBufferSize < ushort.MaxValue);
				var slice = new Slice(buffer, (ushort) requiredBufferSize);
				var result = counters.Read(transaction, slice);

				setStoreBuffer(result);

				counters.Add(transaction, slice, storeBuffer);

				slice = new Slice(buffer, (ushort) counterNameSize);
				result = countersEtagIx.Read(transaction, slice);
				var etagSlice = new Slice(etagBuffer);
				if (result != null) // remove old etag entry
				{
					result.Reader.Read(etagBuffer, 0, sizeof (long));
					etagsCountersIx.Delete(transaction, etagSlice);
				}
				EndianBitConverter.Big.CopyBytes(parent.LastEtag, etagBuffer, 0);
				etagsCountersIx.Add(transaction, etagSlice, slice);
				countersEtagIx.Add(transaction, slice, etagSlice);
			}

			private void EnsureBufferSize(int requiredBufferSize)
			{
				if (buffer.Length < requiredBufferSize)
					buffer = new byte[Utils.NearestPowerOfTwo(requiredBufferSize)];
			}

			private int GetServerId(string server)
			{
				int serverId;
				if (parent.serverNamesToIds.TryGetValue(server, out serverId))
					return serverId;
				serverId = parent.serverNamesToIds.Count;
				parent.serverNamesToIds = new Dictionary<string, int>(parent.serverNamesToIds)
				{
					{server, serverId}
				};
				parent.serverIdstoName = new Dictionary<int, string>(parent.serverIdstoName)
				{
					{serverId,server}
				};
				var servers = transaction.State.GetTree(transaction, "servers");
				servers.Add(transaction, server, EndianBitConverter.Big.GetBytes(serverId));
				return serverId;
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

			public void RecordLastEtagFor(string server, long lastEtag)
			{
				var serverId = GetServerId(server);
				var key = EndianBitConverter.Big.GetBytes(serverId);
				var serversLastEtag = transaction.State.GetTree(transaction, "serversLastEtag");
				serversLastEtag.Add(transaction, new Slice(key), EndianBitConverter.Big.GetBytes(lastEtag));
			}		    
		}

		public string ServerNameFor(int sourceId)
		{
			string value;
			serverIdstoName.TryGetValue(sourceId, out value);
			return value;
		}

        public int SourceIdFor(string serverName)
        {
            int value;
            serverNamesToIds.TryGetValue(serverName, out value);
            return value;
        }

	    public IEnumerable<string> Servers
	    {
	        get { return serverNamesToIds.Keys; }
	    }

	    public class ServerEtag
	    {
	        public int SourceId { get; set; }
	        public long Etag { get; set; }
	    }
	}
}