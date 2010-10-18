using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;

namespace Raven.Munin
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

                    if(cmds.Length == 1 && cmds[0].Type==CommandType.Skip)
                    {
                        persistentSource.Log.Position += cmds[0].Size;
                        continue;
                    }

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

        private Command[] ReadCommands(long lastGoodPosition)
        {
            try
            {
                var cmds = ReadJObject(persistentSource.Log);
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

        private JObject ReadJObject(Stream log)
        {
            return JObject.Load(new BsonReader(log)
            {
                DateTimeKindHandling = DateTimeKind.Utc,
            });
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
                    persistentSource.Log.Position = persistentSource.Log.Length; // always write at the end of the file
                    var cmds = new List<Command>();
                    foreach (var persistentDictionary in dictionaries)
                    {
                        var commandsToCommit = persistentDictionary.GetCommandsToCommit(txId);
                        if (commandsToCommit == null)
                            continue;
                        cmds.AddRange(commandsToCommit);
                    }

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

        private static void WriteCommands(IList<Command> cmds, Stream log)
        {
            if(cmds.Count ==0)
                return;

            var dataSizeInBytes = cmds
                .Where(x => x.Type == CommandType.Put && x.Payload != null)
                .Sum(x => x.Payload.Length);

            if(dataSizeInBytes > 0)
            {
                WriteTo(log, new JArray(new JObject
                {
                    {"type", (byte) CommandType.Skip},
                    {"size", dataSizeInBytes}
                }));
            }

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
                    if(command.Payload != null)
                    {
                        command.Position = log.Position;
                        command.Size = command.Payload.Length;
                        log.Write(command.Payload, 0, command.Payload.Length);
                    }
                    else
                    {
                        command.Position = 0;
                        command.Size = 0;
                    }

                    cmd.Add("position", command.Position);
                    cmd.Add("size", command.Size);
                }

                array.Add(cmd);
            }
            WriteTo(log, array);
        }

        private static void WriteTo(Stream log, JToken jToken)
        {
            jToken.WriteTo(new BsonWriter(log)
            {
                DateTimeKindHandling = DateTimeKind.Unspecified
            });
        }


        public void Compact()
        {
            lock (persistentSource.SyncLock)
            {
                currentlyCommittingLock.EnterWriteLock();
                try
                {
                    Stream tempLog = persistentSource.CreateTemporaryStream();

                    var cmds = new List<Command>();
                    foreach (var persistentDictionary in dictionaries)
                    {
                        cmds.AddRange(persistentDictionary.CopyCommittedData(tempLog));
                        persistentDictionary.ClearCache();
                    }

                    WriteCommands(cmds, tempLog);

                    persistentSource.ReplaceAtomically(tempLog);
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