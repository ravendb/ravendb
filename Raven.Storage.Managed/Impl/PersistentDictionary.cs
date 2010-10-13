using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.Caching;
using System.Threading;
using Newtonsoft.Json.Linq;

namespace Raven.Storage.Managed.Impl
{
    public class PersistentDictionary : IEnumerable<PersistentDictionary.ReadResult>
    {
        private class PositionInFile
        {
            public long Position { get; set; }
            public int Size { get; set; }
            public JToken Key { get; set; }
        }

        public string Name { get; set; }

        public override string ToString()
        {
            return Name + " (" + ItemCount + ")";
        }

        public IEnumerable<JToken> Keys
        {
            get
            {
                currentlyCommittingLock.EnterReadLock();
                try
                {
                    return keyToFilePos.Keys.ToArray();
                }
                finally
                {
                    currentlyCommittingLock.ExitReadLock();
                }
            }
        }

        private readonly ConcurrentDictionary<JToken, PositionInFile> keyToFilePos;

        /// <summary>
        /// We have to support recursion here, because we want to be able to scan a secondary index and pull records from the main one
        /// at the same time.
        /// </summary>
        private ReaderWriterLockSlim currentlyCommittingLock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);

        private readonly List<SecondaryIndex> secondaryIndices = new List<SecondaryIndex>();

        private readonly ConcurrentDictionary<JToken, Guid> keysModifiedInTx;

        private readonly ConcurrentDictionary<Guid, List<Command>> operationsInTransactions = new ConcurrentDictionary<Guid, List<Command>>();

        private readonly IPersistentSource persistentSource;
        private readonly IEqualityComparer<JToken> comparer;

        private readonly MemoryCache cache = new MemoryCache(Guid.NewGuid().ToString());
        public int DictionaryId { get; set; }

        public void JoinToAggregate(ReaderWriterLockSlim theCurrentlyCommittingLock)
        {
            this.currentlyCommittingLock = theCurrentlyCommittingLock;
        }

        public PersistentDictionary(IPersistentSource persistentSource, IEqualityComparer<JToken> comparer)
        {
            keysModifiedInTx = new ConcurrentDictionary<JToken, Guid>(comparer);
            keyToFilePos = new ConcurrentDictionary<JToken, PositionInFile>(comparer);
            this.persistentSource = persistentSource;
            this.comparer = comparer;
        }

        public int WasteCount { get; private set; }

        public int ItemCount
        {
            get { return keyToFilePos.Count; }
        }

        public SecondaryIndex AddSecondaryIndex(Expression<Func<JToken, JToken>> func)
        {
            var secondaryIndex = new SecondaryIndex(new ModifiedJTokenComparer(func.Compile()), func.ToString(),currentlyCommittingLock);
            secondaryIndices.Add(secondaryIndex);
            return secondaryIndex;
        }

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

        public bool Put(JToken key, byte[] value, Guid txId)
        {

            Guid existing;
            if (keysModifiedInTx.TryGetValue(key, out existing) && existing != txId)
                return false;

            long position;
            if(value != null)
            {
                lock (persistentSource.SyncLock)
                {
                    // we *always* write to the end
                    position = persistentSource.Data.Position = persistentSource.Data.Length;
                    persistentSource.Data.Write(value, 0, value.Length);
                }
                cache[position.ToString()] = value;
            }
            else
            {
                position = -1;
                
            }
            operationsInTransactions.GetOrAdd(txId, new List<Command>())
                .Add(new Command
                {
                    Key = key,
                    Position = position,
                    Size = (value == null ? 0 : value.Length),
                    DictionaryId = DictionaryId,
                    Type = CommandType.Put
                });

            if (existing != txId) // otherwise we are already there
                keysModifiedInTx.TryAdd(key, txId);

            return true;
        }

        public bool UpdateKey(JToken key, Guid txId)
        {
            Guid existing;
            if (keysModifiedInTx.TryGetValue(key, out existing) && existing != txId)
                return false;

            var readResult = Read(key, txId);

            if (readResult != null && JTokenComparer.Instance.Equals(readResult.Key, key))
                return true; // no need to do anything, user wrote the same data as is already in, hence, no op

            operationsInTransactions.GetOrAdd(txId, new List<Command>())
                .Add(new Command
                {
                    Key = key,
                    Position = readResult == null ? -1 : readResult.Position,
                    Size = readResult == null ? -1 : readResult.Size,
                    DictionaryId = DictionaryId,
                    Type = CommandType.Put
                });

            if (existing != txId) // otherwise we are already there
                keysModifiedInTx.TryAdd(key, txId);

            return true;
        }

        public ReadResult Read(JToken key, Guid txId)
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
                                Data = () => readData ?? (readData = ReadData(command.Position, command.Size)),
                                Key = command.Key
                            };
                        case CommandType.Delete:
                            return null;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }

            currentlyCommittingLock.EnterReadLock();
            try
            {
                PositionInFile pos;
                if (keyToFilePos.TryGetValue(key, out pos) == false)
                    return null;

                return new ReadResult
                {
                    Position = pos.Position,
                    Size = pos.Size,
                    Data = () => readData ?? (readData = ReadData(pos.Position, pos.Size)),
                    Key = pos.Key
                };
            }
            finally
            {
                currentlyCommittingLock.ExitReadLock();
            }
        }

        private byte[] ReadData(long pos, int size)
        {
            var cacheKey = pos.ToString();
            var cached = cache.Get(cacheKey);
            if (cached != null)
                return (byte[])cached;

            byte[] buf;

            lock (persistentSource.SyncLock)
            {
                cached = cache.Get(cacheKey);
                if (cached != null)
                    return (byte[])cached;

                buf = ReadDataNoCaching(pos, size);
            }

            cache[cacheKey] = buf;

            return buf;
        }

        private byte[] ReadDataNoCaching(long pos, int size)
        {
            persistentSource.Data.Position = pos;

            var read = 0;
            var buf = new byte[size];
            do
            {
                int dataRead = persistentSource.Data.Read(buf, read, buf.Length - read);
                if (dataRead == 0) // nothing read, EOF, probably truncated write, 
                {
                    throw new InvalidDataException("Could not read complete data, the data file is corrupt");
                }
                read += dataRead;
            } while (read < buf.Length);
            return buf;
        }

        internal List<Command> GetCommandsToCommit(Guid txId)
        {
            List<Command> cmds;
            if (operationsInTransactions.TryGetValue(txId, out cmds) == false)
                return null;

            return cmds;
        }

        internal bool CompleteCommit(Guid txId)
        {
            List<Command> cmds;
            if (operationsInTransactions.TryRemove(txId, out cmds) == false || 
                cmds.Count == 0)
                return false;

            ApplyCommands(cmds);
            return true;
        }

        public void Rollback(Guid txId)
        {
            List<Command> commands;
            if (operationsInTransactions.TryRemove(txId, out commands) == false)
                return;

            foreach (Command command in commands)
            {
                Guid _;
                keysModifiedInTx.TryRemove(command.Key, out _);
            }
        }

        public bool Remove(JToken key, Guid txId)
        {
            Guid existing;
            if (keysModifiedInTx.TryGetValue(key, out existing) && existing != txId)
                return false;

            operationsInTransactions.GetOrAdd(txId, new List<Command>())
                .Add(new Command
                {
                    Key = key,
                    DictionaryId = DictionaryId,
                    Type = CommandType.Delete
                });

            if (existing != txId) // otherwise we are already there
                keysModifiedInTx.TryAdd(key, txId);

            return true;
        }

        private void AddInteral(JToken key, PositionInFile position)
        {
            keyToFilePos.AddOrUpdate(key, position, (token, oldPos) =>
            {
                WasteCount += 1;
                foreach (var index in secondaryIndices)
                {
                    index.Remove(oldPos.Key);
                }
                return position;
            });
            foreach (var index in secondaryIndices)
            {
                index.Add(key);
            }
        }

        private void RemoveInternal(JToken key)
        {
            PositionInFile removedValue;
            if (keyToFilePos.TryRemove(key, out removedValue) == false)
                return;
            cache.Remove(removedValue.Position.ToString());
            WasteCount += 1;
            foreach (var index in secondaryIndices)
            {
                index.Remove(removedValue.Key);
            }
        }

        internal void CopyCommittedData(Stream tempData, List<Command> cmds)
        {
            currentlyCommittingLock.EnterReadLock();
            try
            {
                foreach (var kvp in keyToFilePos) // copy committed data
                {
                    long pos = tempData.Position;
                    byte[] data = ReadData(kvp.Value.Position, kvp.Value.Size);

                    byte[] lenInBytes = BitConverter.GetBytes(data.Length);
                    tempData.Write(lenInBytes, 0, lenInBytes.Length);
                    tempData.Write(data, 0, data.Length);

                    cmds.Add(new Command
                    {
                        Key = kvp.Key,
                        Position = pos,
                        DictionaryId = DictionaryId,
                        Size = kvp.Value.Size,
                        Type = CommandType.Put
                    });

                    kvp.Value.Position = pos;
                }
            }
            finally
            {
                currentlyCommittingLock.ExitReadLock();
            }
        }

        public void CopyUncommitedData(Stream tempData)
        {
            // copy uncommitted data
            foreach (Command uncommitted in operationsInTransactions
                .SelectMany(x => x.Value)
                .Where(x => x.Type == CommandType.Put))
            {
                long pos = tempData.Position;
                byte[] data = ReadData(uncommitted.Position, uncommitted.Size);

                byte[] lenInBytes = BitConverter.GetBytes(data.Length);
                tempData.Write(lenInBytes, 0, lenInBytes.Length);
                tempData.Write(data, 0, data.Length);

                uncommitted.Position = pos;
            }
        }

        public void ClearCache()
        {
            cache.Trim(percent: 100);
        }



        public class ReadResult
        {
            public int Size { get; set; }
            public long Position { get; set; }
            public JToken Key { get; set; }
            public Func<byte[]> Data { get; set; }
        }

        public IEnumerator<ReadResult> GetEnumerator()
        {
            foreach (var positionInFile in keyToFilePos.Values.ToArray())
            {
                byte[] readData = null;
                var pos = positionInFile;
                yield return new ReadResult
                {
                    Key = positionInFile.Key,
                    Position = positionInFile.Position,
                    Size = positionInFile.Size,
                    Data = () => readData ?? (readData = ReadData(pos.Position, pos.Size)),
               
                };
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}