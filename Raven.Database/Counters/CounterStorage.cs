using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Text;
using Raven.Abstractions;
using Raven.Database.Config;
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
		private readonly StorageEnvironment storageEnvironment;
		public Guid Id { get; private set; }
		public DateTime LastWrite { get; private set; }

		private readonly Dictionary<string, int> serverIds = new Dictionary<string, int>(); 

		private long lastEtag;

		public CounterStorage(string name, InMemoryRavenConfiguration configuration)
		{
			var options = configuration.RunInMemory ? StorageEnvironmentOptions.CreateMemoryOnly()
				: CreateStorageOptionsFromConfiguration(configuration.CountersDataDirectory, configuration.Settings);

			storageEnvironment = new StorageEnvironment(options);

			Initialize(name);
		}

		private void Initialize(string name)
		{
			using (var tx = storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
			{
				storageEnvironment.CreateTree(tx, "counters");

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
					serverIds[name] = 0;
					metadata.Add(tx, "id", Id.ToByteArray());
					metadata.Add(tx, "name", Encoding.UTF8.GetBytes(name));
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
								serverIds[it.CurrentKey.ToString()] = it.CreateReaderForCurrent().ReadInt32();
							} while (it.MoveNext());
						}
					}
					using (var it = etags.Iterate(tx))
					{
						if (it.Seek(Slice.AfterAllKeys))
						{
							lastEtag = it.CurrentKey.ToInt64();
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
			return new Reader(storageEnvironment);
		}

		public Writer CreateWriter()
		{
			LastWrite = SystemTime.UtcNow;
			return new Writer(this, storageEnvironment);
		}

		public void Dispose()
		{
			
			if (storageEnvironment != null)
				storageEnvironment.Dispose();
		}

		public class Reader : IDisposable
		{
			private readonly Transaction transaction;
			private readonly Tree countersTree, countersEtags;
			private readonly byte[] serverIdBytes = new byte[sizeof (int)];

			public Reader(StorageEnvironment storageEnvironment)
			{
				transaction = storageEnvironment.NewTransaction(TransactionFlags.Read);
				countersTree = transaction.State.GetTree(transaction, "counters");
				countersEtags = transaction.State.GetTree(transaction, "counters->etags");
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

			public Counter GetCounter(string name)
			{
				Slice slice = name;
				var etagResult = countersEtags.Read(transaction, slice);
				if (etagResult == null)
					return null;
				var etag = etagResult.Reader.ReadInt64();
				using (var it = countersTree.Iterate(transaction))
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
			private readonly Tree counters, etagsCountersIx, countersEtagIx;
			private readonly byte[] storeBuffer;
			private byte[] buffer = new byte[0];
			private bool incrementedEtag;
			private readonly byte[] etagBuffer = new byte[sizeof(long)];

			public Writer(CounterStorage parent, StorageEnvironment storageEnvironment)
			{
				this.parent = parent;
				transaction = storageEnvironment.NewTransaction(TransactionFlags.ReadWrite);
				counters = transaction.State.GetTree(transaction, "counters");
				etagsCountersIx = transaction.State.GetTree(transaction, "etags->counters");
				countersEtagIx = transaction.State.GetTree(transaction, "counters->etags");
			
				storeBuffer = new byte[sizeof(long) + //positive
				                       sizeof(long)]; // negative
			}

			public void Store(string server, string counter, long delta)
			{
				if (incrementedEtag == false)
				{
					parent.lastEtag ++;
					incrementedEtag = true;
				}
				var serverId = GetServerId(server);

				var counterNameSize = Encoding.UTF8.GetByteCount(counter);
				var requiredBufferSize = counterNameSize + sizeof(int);
				EnsureBufferSize(requiredBufferSize);

				var end = Encoding.UTF8.GetBytes(counter, 0, counter.Length, buffer, 0);
				EndianBitConverter.Big.CopyBytes(serverId, buffer, end);

				Debug.Assert(requiredBufferSize < ushort.MaxValue);
				var slice = new Slice(buffer, (ushort)requiredBufferSize);
				var valPos = delta > 0 ? 0 : 8;
				var result = counters.Read(transaction, slice);
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
				counters.Add(transaction, slice, storeBuffer);

				slice = new Slice(storeBuffer, (ushort) counterNameSize);
				result = countersEtagIx.Read(transaction, slice);
				var etagSlice = new Slice(etagBuffer);
				if (result != null) // remove old etag entry
				{
					result.Reader.Read(etagBuffer, 0, sizeof (long));
					etagsCountersIx.Delete(transaction, etagSlice);
				}
				EndianBitConverter.Big.CopyBytes(parent.lastEtag, etagBuffer, 0);
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
				if (parent.serverIds.TryGetValue(server, out serverId))
					return serverId;
				serverId = parent.serverIds.Count;
				parent.serverIds[server] = serverId;
				var servers = transaction.State.GetTree(transaction, "servers");
				servers.Add(transaction, server, BitConverter.GetBytes(serverId));
				return serverId;
			}

			public void Commit()
			{
				transaction.Commit();
			}

			public void Dispose()
			{
				parent.LastWrite = SystemTime.UtcNow;
				if (transaction != null)
					transaction.Dispose();
			}
		}
	}
}