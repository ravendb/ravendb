//-----------------------------------------------------------------------
// <copyright file="Database.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using Raven.Imports.Newtonsoft.Json.Bson;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;

namespace Raven.Munin
{
	public class Database : IEnumerable<Table>, IDisposable
	{
		private readonly IPersistentSource persistentSource;
		private readonly List<Table> tables = new List<Table>();

		public List<Table> Tables
		{
			get { return tables; }
		}

		public readonly ThreadLocal<Guid> CurrentTransactionId = new ThreadLocal<Guid>(() => Guid.Empty);

		internal IList<PersistentDictionaryState> DictionaryStates
		{
			get { return persistentSource.DictionariesStates; }
		}

		public Database(IPersistentSource persistentSource)
		{
			this.persistentSource = persistentSource;
		}

		const int version = 1;

		public void Initialze()
		{
			persistentSource.Write(log =>
			{
				if (log.Length == 0) // new file
				{
					WriteFileHeader(log);
					log.Flush();
				}
				log.Position = 0;
				
				AssertValidVersionAndTables(log);

				while (true)
				{
					long lastGoodPosition = log.Position;

					if (log.Position == log.Length)
						break;// EOF

					var cmds = ReadCommands(log, lastGoodPosition);
					if (cmds == null)
						break;

					if (cmds.Length == 1 && cmds[0].Type == CommandType.Skip)
					{
						log.Position += cmds[0].Size;
						continue;
					}

					foreach (var commandForDictionary in cmds.GroupBy(x => x.DictionaryId))
					{
						tables[commandForDictionary.Key].ApplyCommands(commandForDictionary);
					}
				}
			});
		}

		private void WriteFileHeader(Stream log)
		{
			new RavenJObject
			{
				{"Version", version},
				{"Tables", new RavenJArray(Tables.Select(x=>x.Name).ToArray())}
			}.WriteTo(new BsonWriter(log));
		}

		private void AssertValidVersionAndTables(Stream log)
		{
			try
			{
				var versionInfo = (RavenJObject)RavenJToken.ReadFrom(new BsonReader(log));

				if (versionInfo.Value<int>("Version") != version)
					throw new InvalidOperationException("Invalid Munin file version!");

				var tableNames = versionInfo.Value<RavenJArray>("Tables");

				if (tableNames.Length != tables.Count)
					throw new InvalidOperationException("Different number of tables stored in the Munin file");

				for (int i = 0; i < tableNames.Length; i++)
				{
					if (tableNames[i].Value<string>() != tables[i].Name)
						throw new InvalidOperationException("Table at position " + i + " is expected to be " + tables[i].Name + " but was actually " + tableNames[i]);
				}
			}
			catch (Exception e)
			{
				throw new InvalidOperationException(
					"Could not open Munin data file, probably not a Munin file or an out of date file", e);
			}
		}

		private static Command[] ReadCommands(Stream log, long lastGoodPosition)
		{
			try
			{
				var cmds = ReadJObject(log);
				return cmds.Values<RavenJToken>().Select(cmd => new Command
				{
					Key = cmd.Value<RavenJToken>("key"),
					Position = cmd.Value<long>("position"),
					Size = cmd.Value<int>("size"),
					Type = (CommandType)cmd.Value<byte>("type"),
					DictionaryId = cmd.Value<int>("dicId")
				}).ToArray();
			}
			catch (Exception)
			{
				log.SetLength(lastGoodPosition);//truncate log to last known good position
				return null;
			}
		}

		private static RavenJObject ReadJObject(Stream log)
		{
			return RavenJObject.Load(new BsonReader(log)
			{
				DateTimeKindHandling = DateTimeKind.Utc,
			});
		}

		public Table this[int i]
		{
			get { return tables[i]; }
		}

		public IDisposable BeginTransaction()
		{
			if (CurrentTransactionId.Value != Guid.Empty)
				return new DisposableAction(() => { }); // no op, already in tx

			CurrentTransactionId.Value = Guid.NewGuid();
			return new DisposableAction(() =>
			{
				if (CurrentTransactionId.Value != Guid.Empty) // tx not committed
					Rollback();
			});
		}

		public IDisposable SuppressTransaction()
		{
			var old = CurrentTransactionId.Value;

			CurrentTransactionId.Value = Guid.Empty;

			return new DisposableAction(() =>
			{
				CurrentTransactionId.Value = old;
			});
		}

		[DebuggerNonUserCode]
		public void Commit()
		{
			if (CurrentTransactionId.Value == Guid.Empty)
				return;

			Commit(CurrentTransactionId.Value);

			CurrentTransactionId.Value = Guid.Empty;
		}

		public void Rollback()
		{
			Rollback(CurrentTransactionId.Value);

			CurrentTransactionId.Value = Guid.Empty;
		}

		private void Commit(Guid txId)
		{
			// this assume that munin transactions always use a single thread

			var cmds = new List<Command>();
			foreach (var table in tables)
			{
				var commandsToCommit = table.GetCommandsToCommit(txId);
				if (commandsToCommit == null)
					continue;
				cmds.AddRange(commandsToCommit);
			}

			if (cmds.Count == 0)
				return;

			persistentSource.Write(log =>
			{
				log.Position = log.Length; // always write at the end of the file

				for (int i = 0; i < cmds.Count; i+=1024)
				{
					WriteCommands(cmds.Skip(i).Take(1024), log);
					
				}
				persistentSource.FlushLog(); // flush all the index changes to disk

				foreach (var table in tables)
				{
					table.CompleteCommit(txId);
				}

			});
		}

		private void Rollback(Guid txId)
		{
			foreach (var persistentDictionary in tables)
			{
				persistentDictionary.Rollback(txId);
			}
		}

		private static void WriteCommands(IEnumerable<Command> cmds, Stream log)
		{
			const int shaSize = 32;
			var dataSizeInBytes = cmds
				.Where(x => x.Type == CommandType.Put && x.Payload != null)
				.Sum(x => x.Payload.Length + shaSize);

			if (dataSizeInBytes > 0)
			{
				WriteTo(log, new RavenJArray(new RavenJObject {{"type", (short) CommandType.Skip}, {"size", dataSizeInBytes}}));
			}

			var array = new RavenJArray();
			foreach (var command in cmds)
			{

				var cmd = new RavenJObject
				          	{
								{"type", (short) command.Type},
								{"key", command.Key},
								{"dicId", command.DictionaryId}
				          	};

				if (command.Type == CommandType.Put)
				{
					if (command.Payload != null)
					{
						command.Position = log.Position;
						command.Size = command.Payload.Length;
						log.Write(command.Payload, 0, command.Payload.Length);
						using (var sha256 = SHA256.Create())
						{
							var sha = sha256.ComputeHash(command.Payload);
							log.Write(sha, 0, sha.Length);
						}
					}
					cmd.Add("position", command.Position);
					cmd.Add("size", command.Size);
				}

				array.Add(cmd);
			}

			if (array.Length == 0 && dataSizeInBytes == 0)
				return;
			WriteTo(log, array);
		}

		private static void WriteTo(Stream log, RavenJToken jToken)
		{
			jToken.WriteTo(new BsonWriter(log)
			{
				DateTimeKindHandling = DateTimeKind.Unspecified
			});
		}


		public void Compact()
		{
			persistentSource.Write(log =>
			{
				Stream tempLog = persistentSource.CreateTemporaryStream();

				WriteFileHeader(tempLog);
			  
				var cmds = new List<Command>();
				foreach (var persistentDictionary in tables)
				{
					cmds.AddRange(persistentDictionary.CopyCommittedData(tempLog));
					persistentDictionary.ResetWaste();
				}

				WriteCommands(cmds, tempLog);

				persistentSource.ClearPool();

				persistentSource.ReplaceAtomically(tempLog);

				using (SuppressTransaction())
				{
					CurrentTransactionId.Value = Guid.NewGuid();
					foreach (var command in cmds)
					{
						tables[command.DictionaryId].UpdateKey(command.Key, command.Position, command.Size);
						tables[command.DictionaryId].CompleteCommit(CurrentTransactionId.Value);
					}
				}
			});
		}


		/// <summary>
		/// This method should be called when the application is idle
		/// It is used for book keeping tasks such as compacting the data storage file.
		/// </summary>
		public void PerformIdleTasks()
		{
			if (CompactionRequired() == false)
				return;

			Compact();
		}


		private bool CompactionRequired()
		{
			var itemsCount = tables.Sum(x => x.Count);
			var wasteCount = tables.Sum(x => x.WasteCount);

			if (itemsCount < 10000) // for small data sizes, we cleanup on 100% waste
				return wasteCount > itemsCount;
			if (itemsCount < 100000) // for medium data sizes, we cleanup on 50% waste
				return wasteCount > (itemsCount / 2);
			return wasteCount > (itemsCount / 10); // on large data size, we cleanup on 10% waste
		}

		public Table Add(Table dictionary)
		{
			tables.Add(dictionary);
			DictionaryStates.Add(null);
			dictionary.Initialize(persistentSource, tables.Count - 1, this, CurrentTransactionId);
			return dictionary;
		}

		public IEnumerator<Table> GetEnumerator()
		{
			return tables.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		/// <filterpriority>2</filterpriority>
		public virtual void Dispose()
		{
			foreach (var table in Tables)
			{
				table.Dispose();
			}
			CurrentTransactionId .Dispose();
		}
	}
}