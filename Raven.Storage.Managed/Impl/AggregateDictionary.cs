using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;
using Raven.Database.Json;

namespace Raven.Storage.Managed.Impl
{
    public class AggregateDictionary : IEnumerable<PersistentDictionary>
    {
        private readonly IPersistentSource persistentSource;
        private readonly List<PersistentDictionary> dictionaries = new List<PersistentDictionary>();
        private ReaderWriterLockSlim currentlyCommittingLock;

        public AggregateDictionary(IPersistentSource persistentSource)
        {
            this.persistentSource = persistentSource;
        }


        public void Initialze()
        {
            currentlyCommittingLock.EnterWriteLock();
            try
            {
                while (true)
                {
                    long lastGoodPosition = persistentSource.Log.Position;

                    if (persistentSource.Log.Position == persistentSource.Log.Length)
                        break;// EOF

                    var cmds = ReadCommands(lastGoodPosition);
                    if (cmds == null)
                        break;

                    foreach (var commandForDictionary in cmds.GroupBy(x => x.DictionaryId))
                    {
                        dictionaries[commandForDictionary.Key].ApplyCommands(commandForDictionary);
                    }
                }
            }
            finally
            {
                currentlyCommittingLock.ExitWriteLock();
            }
        }

        private IEnumerable<Command> ReadCommands(long lastGoodPosition)
        {
            try
            {
                var cmds = persistentSource.Log.ToJObject();
                return cmds.Values().Select(cmd => new Command
                {
                    Key = cmd.Value<JToken>("key"),
                    Position = cmd.Value<long>("position"),
                    Size = cmd.Value<int>("size"),
                    Type = (CommandType)cmd.Value<byte>("type"),
                    DictionaryId = cmd.Value<int>("dicId")
                }).ToArray();
            }
            catch (Exception)
            {
                persistentSource.Log.SetLength(lastGoodPosition);//truncate log to last known good position
                return null;
            }
        }

        public PersistentDictionary this[int i]
        {
            get { return dictionaries[i]; }
        }

        [DebuggerNonUserCode]
        public bool Commit(Guid txId)
        {
            lock (persistentSource.SyncLock)
            {
                currentlyCommittingLock.EnterWriteLock();
                try
                {
                    var cmds = new List<Command>();
                    foreach (var persistentDictionary in dictionaries)
                    {
                        var commandsToCommit = persistentDictionary.GetCommandsToCommit(txId);
                        if (commandsToCommit == null)
                            continue;
                        cmds.AddRange(commandsToCommit);
                    }

                    persistentSource.FlushData(); // sync the data to disk before doing anything else
                    WriteCommands(cmds, persistentSource.Log);
                    persistentSource.FlushLog(); // flush all the index changes to disk

                    return dictionaries.Aggregate(false, (changed, persistentDictionary) => changed | persistentDictionary.CompleteCommit(txId));


                }
                finally
                {
                    currentlyCommittingLock.ExitWriteLock();
                }
            }
        }

        public void Rollback(Guid txId)
        {
            foreach (var persistentDictionary in dictionaries)
            {
                persistentDictionary.Rollback(txId);
            }
        }

        private static void WriteCommands(IEnumerable<Command> cmds, Stream log)
        {
            var array = new JArray();
            foreach (var command in cmds)
            {
                var cmd = new JObject
                {
                    {"type", (byte) command.Type},
                    {"key", command.Key},
                    {"dicId", command.DictionaryId}
                };

                if (command.Type == CommandType.Put)
                {
                    cmd.Add("position", command.Position);
                    cmd.Add("size", command.Size);
                }

                array.Add(cmd);
            }
            if (array.Count == 0)
                return;
            array.WriteTo(log);
        }


        public void Compact()
        {
            lock (persistentSource.SyncLock)
            {
                currentlyCommittingLock.EnterWriteLock();
                try
                {
                    Stream tempLog = persistentSource.CreateTemporaryStream();
                    Stream tempData = persistentSource.CreateTemporaryStream();

                    var cmds = new List<Command>();
                    foreach (var persistentDictionary in dictionaries)
                    {
                        persistentDictionary.CopyCommittedData(tempData, cmds);
                        persistentDictionary.CopyUncommitedData(tempData);
                        persistentDictionary.ClearCache();
                    }

                    WriteCommands(cmds, tempLog);

                    persistentSource.ReplaceAtomically(tempData, tempLog);
                }
                finally
                {
                    currentlyCommittingLock.ExitWriteLock();
                }
            }
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
            var itemsCount = dictionaries.Sum(x => x.ItemCount);
            var wasteCount = dictionaries.Sum(x => x.WasteCount);

            if (itemsCount < 10000) // for small data sizes, we cleanup on 100% waste
                return wasteCount > itemsCount;
            if (itemsCount < 100000) // for meduim data sizes, we cleanup on 50% waste
                return wasteCount > (itemsCount / 2);
            return wasteCount > (itemsCount / 10); // on large data size, we cleanup on 10% waste
        }

        public PersistentDictionary Add(PersistentDictionary dictionary)
        {
            currentlyCommittingLock = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
            dictionary.JoinToAggregate(currentlyCommittingLock);
            dictionaries.Add(dictionary);
            dictionary.DictionaryId = dictionaries.Count - 1;
            return dictionary;
        }

        public IEnumerator<PersistentDictionary> GetEnumerator()
        {
            return dictionaries.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}