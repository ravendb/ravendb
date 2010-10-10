using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Caching;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;

namespace Raven.Storage.Managed.Impl
{
    public class PersistentDictionary
    {
        private class PositionInFile
        {
            public long Position { get; set; }
            public int Size { get; set; }
            public JToken Key { get; set; }
        }

        private readonly ConcurrentDictionary<JToken, PositionInFile> keyToFilePos;

        private readonly List<SecondaryIndex> secondaryIndices = new List<SecondaryIndex>();

        private readonly ConcurrentDictionary<JToken, Guid> keysModifiedInTx;

        private readonly ConcurrentDictionary<Guid, List<Command>> operationsInTransactions = new ConcurrentDictionary<Guid, List<Command>>();

        private readonly IPersistentSource persistentSource;
        private readonly IEqualityComparer<JToken> comparer;

        private readonly MemoryCache cache = new MemoryCache(Guid.NewGuid().ToString());
        public int DictionaryId { get; set; }

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

        public SecondaryIndex AddSecondaryIndex(Func<JToken, JToken> func)
        {
            var secondaryIndex = new SecondaryIndex(new ModifiedJTokenComparer(func));
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
            }
        }

        public bool Put(JToken key, byte[] value, Guid txId)
        {

            Guid existing;
            if (keysModifiedInTx.TryGetValue(key, out existing) && existing != txId)
                return false;

            long position;
            lock (persistentSource.SyncLock)
            {
                // we *always* write to the end
                position = persistentSource.Data.Position = persistentSource.Data.Length;
                persistentSource.Data.Write(value, 0, value.Length);
            }
            cache[position.ToString()] = value;
            operationsInTransactions.GetOrAdd(txId, new List<Command>())
                .Add(new Command
                {
                    Key = key,
                    Position = position,
                    Size = value.Length,
                    DictionaryId = DictionaryId,
                    Type = CommandType.Put
                });

            if (existing != txId) // otherwise we are already there
                keysModifiedInTx.TryAdd(key, txId);

            return true;
        }

        public ReadResult Read(JToken key, Guid txId)
        {
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
                                Data = ReadData(command.Position, command.Size),
                                Key = command.Key
                            };
                        case CommandType.Delete:
                            return null;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
            }

            PositionInFile pos;
            if (keyToFilePos.TryGetValue(key, out pos) == false)
                return null;

            return new ReadResult
            {
                Data = ReadData(pos.Position, pos.Size),
                Key = pos.Key
            };
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

        internal void CompleteCommit(Guid txId)
        {
            List<Command> cmds;
            if (operationsInTransactions.TryGetValue(txId, out cmds) == false)
                return;

            ApplyCommands(cmds);
            ClearTransactionInMemoryData(txId);
        }

        public void Rollback(Guid txId)
        {
            ClearTransactionInMemoryData(txId);
        }

        private void ClearTransactionInMemoryData(Guid txId)
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
                return position;
            });
            foreach (var index in secondaryIndices)
            {
                index.Add(key);
            }
        }

        private void RemoveInternal(JToken key)
        {
            PositionInFile _;
            keyToFilePos.TryRemove(key, out _);
            WasteCount += 1;
            foreach (var index in secondaryIndices)
            {
                index.Remove(key);
            }
        }

        internal void CopyCommittedData(Stream tempData, List<Command> cmds)
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
            public JToken Key { get; set; }
            public byte[] Data { get; set; }
        }
    }

    public class SecondaryIndex
    {
        private readonly SortedList<JToken, object> index;

        public SecondaryIndex(IComparer<JToken> comparer)
        {
            this.index = new SortedList<JToken, object>(comparer);
        }

        public void Add(JToken key)
        {
            lock (index)
                index[key] = null;
        }

        public void Remove(JToken key)
        {
            lock (index)
                index.Remove(key);
        }


        public IEnumerable<JToken> SkipFromEnd(int start)
        {
            lock (index)
            {
                for (int i = (index.Count - 1) - start; i >= 0; i--)
                {
                    yield return index.Keys[i];
                }
            }
        }

        public IEnumerable<JToken> SkipAfter(JToken key)
        {
            lock(index)
            {
                var recordingComparer = new RecordingComparer();
                Array.BinarySearch(index.Keys.ToArray(), key, recordingComparer);

                if(recordingComparer.LastComparedTo == null)
                    yield break;

                var indexOf = index.IndexOfKey(recordingComparer.LastComparedTo);

                for (int i = indexOf; i < index.Count; i++)
                {
                    yield return index.Keys[i];
                }
            }
        }

        
    }

}