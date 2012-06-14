//-----------------------------------------------------------------------
// <copyright file="Table.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Security.Cryptography;
using System.Threading;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Munin.Tree;

namespace Raven.Munin
{
	public class Table : IEnumerable<Table.ReadResult>, IDisposable
	{
		private ThreadLocal<Guid> _txId;
		public string Name { get; set; }

		public IEnumerable<RavenJToken> Keys
		{
			get
			{
				return persistentSource.Read(() => KeyToFilePos.KeysInOrder);
			}
		}

		private IBinarySearchTree<RavenJToken, PositionInFile> KeyToFilePos
		{
			get { return parent.DictionaryStates[TableId].KeyToFilePositionInFiles; }
			set { parent.DictionaryStates[TableId].KeyToFilePositionInFiles = value; }
		}


		private readonly ConcurrentDictionary<RavenJToken, Guid> keysModifiedInTx;

		private readonly ConcurrentDictionary<Guid, List<Command>> operationsInTransactions = new ConcurrentDictionary<Guid, List<Command>>();

		private IPersistentSource persistentSource;
		private readonly IComparerAndEquality<RavenJToken> comparer;

		private Database parent;
		public int TableId { get; set; }

		public void Add(string name, Expression<Func<RavenJToken, IComparable>> func)
		{
			var secondaryIndex = new SecondaryIndex(func.Compile(), func.ToString())
			{
				Name = name
			};
			SecondaryIndices.Add(secondaryIndex);
		}

		public Table(string name) : this(RavenJTokenComparer.Instance, name)
		{
		}

		public Table(Func<RavenJToken, RavenJToken> clusteredIndexeExtractor, string name)
			: this(new ModifiedJTokenComparer(clusteredIndexeExtractor), name)
		{
			
		}

		private Table(IComparerAndEquality<RavenJToken> comparer, string name)
		{
			keysModifiedInTx = new ConcurrentDictionary<RavenJToken, Guid>(comparer);
			this.comparer = comparer;
			Name = name;
			SecondaryIndices = new List<SecondaryIndex>();
		}

		public int WasteCount { get; private set; }

		public void ResetWaste()
		{
			WasteCount = 0;
		}

		public int Count
		{
			get { return KeyToFilePos.Count; }
		}

		public List<SecondaryIndex> SecondaryIndices { get; set; }

		internal void ApplyCommands(IEnumerable<Command> cmds)
		{
			foreach (Command command in cmds)
			{
				switch (command.Type)
				{
					case CommandType.Put:
						AddInteral(command.Key, new PositionInFile
						{
							Position = command.Position,
							Size = command.Size,
							Key = command.Key
						});
						break;
					case CommandType.Delete:
						RemoveInternal(command.Key);
						break;
					default:
						throw new ArgumentOutOfRangeException();
				}
				Guid _;
				keysModifiedInTx.TryRemove(command.Key, out _);
			}
		}

		public ReadResult Read(RavenJToken key)
		{
			return Read(key, _txId.Value);
		}


		public bool UpdateKey(RavenJToken key)
		{
			return UpdateKey(key, _txId.Value);
		}
		
		public SecondaryIndex this[string indexName]
		{
			get { return SecondaryIndices.First(x=>x.Name == indexName); }
		}

		public bool Remove(RavenJToken key)
		{
			return Remove(key, _txId.Value);
		}

		public bool Put(RavenJToken key, byte[] value)
		{
			return Put(key, value, _txId.Value);
		}

		public bool Put(RavenJToken key, byte[] value, Guid transactionId)
		{
			Guid existing;
			if (keysModifiedInTx.TryGetValue(key, out existing) && existing != transactionId)
				return false;

			operationsInTransactions.GetOrAdd(transactionId, new List<Command>())
				.Add(new Command
				{
					Key = key,
					Payload = value,
					DictionaryId = TableId,
					Type = CommandType.Put
				});

			if (existing != transactionId) // otherwise we are already there
				keysModifiedInTx.TryAdd(key, transactionId);

			return true;
		}

		internal bool UpdateKey(RavenJToken key, Guid txId)
		{
			Guid existing;
			if (keysModifiedInTx.TryGetValue(key, out existing) && existing != txId)
				return false;

			var readResult = Read(key, txId);

			if (readResult != null && RavenJToken.DeepEquals(key, readResult.Key))
				return true; // no need to do anything, user wrote the same data as is already in, hence, no op

			operationsInTransactions.GetOrAdd(txId, new List<Command>())
				.Add(new Command
				{
					Key = key,
					Position = readResult == null ? -1 : readResult.Position,
					Size = readResult == null ? 0 : readResult.Size,
					DictionaryId = TableId,
					Type = CommandType.Put
				});

			if (existing != txId) // otherwise we are already there
				keysModifiedInTx.TryAdd(key, txId);

			return true;
		}

		internal ReadResult Read(RavenJToken key, Guid txId)
		{
			byte[] readData = null;

			Guid mofiedByTx;
			if (keysModifiedInTx.TryGetValue(key, out mofiedByTx) && mofiedByTx == txId)
			{
				Command command = operationsInTransactions.GetOrAdd(txId, new List<Command>()).LastOrDefault(
					x => comparer.Equals(x.Key, key));

				if (command != null)
				{
					switch (command.Type)
					{
						case CommandType.Put:
							return new ReadResult
							{
								Position = command.Position,
								Size = command.Size,
								Data = () => readData ?? (readData = ReadData(command)),
								Key = command.Key
							};
						case CommandType.Delete:
							return null;
						default:
							throw new ArgumentOutOfRangeException();
					}
				}
			}

			return persistentSource.Read(log =>
			{
				PositionInFile pos;
				if (KeyToFilePos.TryGetValue(key, out pos) == false)
					return null;

				return new ReadResult
				{
					Position = pos.Position,
					Size = pos.Size,
					Data = () => readData ?? (readData = ReadData(pos.Position, pos.Size, pos.Key)),
					Key = pos.Key
				};
			});
		}

		private byte[] ReadData(Command command)
		{
			if (command.Payload != null)
				return command.Payload;

			return ReadData(command.Position, command.Size, command.Key);
		}

		private byte[] ReadData(long pos, int size, RavenJToken key)
		{
			if (pos == -1)
				return null;

			return persistentSource.Read(log =>
			{
				return ReadDataNoCaching(log, pos, size, key);
			});
		}

		private byte[] ReadDataNoCaching(Stream log, long pos, int size, RavenJToken key)
		{
			log.Position = pos;
			var binaryReader = new BinaryReader(log);
			var data = binaryReader.ReadBytes(size);
			if(data.Length != size)
				throw new InvalidDataException("Could not read complete data, the file is probably corrupt when reading: " + key.ToString(Formatting.None) + " on table " + Name);

			using(var sha256 = SHA256.Create())
			{
				var hash = sha256.ComputeHash(data);
				var hashFromFile = binaryReader.ReadBytes(hash.Length);
				if(hashFromFile.Length != hash.Length)
					throw new InvalidDataException("Could not read complete SHA data, the file is probably corrupt when reading: " + key.ToString(Formatting.None) + " on table " + Name);

				if (hashFromFile.Where((t, i) => hash[i] != t).Any())
				{
					throw new InvalidDataException("Invalid SHA signature, the file is probably corrupt when reading: " + key.ToString(Formatting.None) + " on table " + Name);
				}
			}

			return data;
		}

		internal List<Command> GetCommandsToCommit(Guid transactionId)
		{
			List<Command> cmds;
			if (operationsInTransactions.TryGetValue(transactionId, out cmds) == false)
				return null;

			return cmds;
		}

		public bool CompleteCommit(Guid transactionId)
		{
			List<Command> cmds;
			if (operationsInTransactions.TryRemove(transactionId, out cmds) == false || 
				cmds.Count == 0)
				return false;

			ApplyCommands(cmds);
			return true;
		}

		public void Rollback(Guid transactionId)
		{
			List<Command> commands;
			if (operationsInTransactions.TryRemove(transactionId, out commands) == false)
				return;

			foreach (Command command in commands)
			{
				Guid _;
				keysModifiedInTx.TryRemove(command.Key, out _);
			}
		}

		public bool Remove(RavenJToken key, Guid transactionId)
		{
			Guid existing;
			if (keysModifiedInTx.TryGetValue(key, out existing) && existing != transactionId)
				return false;

			operationsInTransactions.GetOrAdd(transactionId, new List<Command>())
				.Add(new Command
				{
					Key = key,
					DictionaryId = TableId,
					Type = CommandType.Delete
				});

			if (existing != transactionId) // otherwise we are already there
				keysModifiedInTx.TryAdd(key, transactionId);

			return true;
		}

		private void AddInteral(RavenJToken key, PositionInFile position)
		{
			KeyToFilePos = KeyToFilePos.AddOrUpdate(key, position, (token, oldPos) =>
			{
				WasteCount += 1;
				foreach (var index in SecondaryIndices)
				{
					index.Remove(oldPos.Key);
				}
				return position;
			});
			foreach (var index in SecondaryIndices)
			{
				index.Add(key);
			}
		}

		private void RemoveInternal(RavenJToken key)
		{
			PositionInFile removedValue;
			bool removed;
			KeyToFilePos = KeyToFilePos.TryRemove(key, out removed, out removedValue);
			if (removed == false)
				return;
			WasteCount += 1;
			foreach (var index in SecondaryIndices)
			{
				index.Remove(removedValue.Key);
			}
		}

		internal IEnumerable<Command> CopyCommittedData(Stream tempData)
		{
			return from kvp in KeyToFilePos.Pairs
				   select new Command
				   {
					   Key = kvp.Key,
					   Payload = ReadData(kvp.Value.Position, kvp.Value.Size, kvp.Key),
					   DictionaryId = TableId,
					   Size = kvp.Value.Size,
					   Type = CommandType.Put
				   };
		}

		public class ReadResult
		{
			public int Size { get; set; }
			public long Position { get; set; }
			public RavenJToken Key { get; set; }
			public Func<byte[]> Data { get; set; }
		}

		public IEnumerator<ReadResult> GetEnumerator()
		{
			foreach (var positionInFile in KeyToFilePos.ValuesInOrder)
			{
				byte[] readData = null;
				var pos = positionInFile;
				yield return new ReadResult
				{
					Key = positionInFile.Key,
					Position = positionInFile.Position,
					Size = positionInFile.Size,
					Data = () => readData ?? (readData = ReadData(pos.Position, pos.Size, pos.Key)),
			   
				};
			}
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		public void Initialize(IPersistentSource source, int tableId, Database database, ThreadLocal<Guid> transactionId)
		{
			persistentSource = source;
			TableId = tableId;
			parent = database;
			_txId = transactionId;

			parent.DictionaryStates[tableId] = new PersistentDictionaryState(comparer);

			int index = 0;
			foreach (var secondaryIndex in SecondaryIndices)
			{
				persistentSource.DictionariesStates[TableId].SecondaryIndicesState.Add(new EmptyAVLTree<IComparable, IBinarySearchTree<RavenJToken, RavenJToken>>(Comparer<IComparable>.Default, x => x, x => x));
				secondaryIndex.Initialize(persistentSource, TableId, index++);
			}
		}

		public bool UpdateKey(RavenJToken key, long position, int size)
		{
			Guid existing;
			if (keysModifiedInTx.TryGetValue(key, out existing) && existing != _txId.Value)
				return false;

			operationsInTransactions.GetOrAdd(_txId.Value, new List<Command>())
				.Add(new Command
				{
					Key = key,
					Position = position,
					Size = size,
					DictionaryId = TableId,
					Type = CommandType.Put
				});

			if (existing != _txId.Value) // otherwise we are already there
				keysModifiedInTx.TryAdd(key, _txId.Value);

			return true;
		}

		public void Dispose()
		{
		}
	}
}