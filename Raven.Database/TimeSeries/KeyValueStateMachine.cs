using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Rachis.Commands;
using Rachis.Interfaces;
using Rachis.Messages;
using Voron;
using Voron.Impl.Backup;
using Voron.Util.Conversion;

namespace Raven.Database.TimeSeries
{
    public class OperationBatchCommand : Command
    {
        public KeyValueOperation[] Batch { get; set; }
    }

    public class GetCommand : Command
    {
        public string Key { get; set; }
    }

    public class CasCommand : Command
    {
        public string Key { get; set; }
        public JToken Value;
        public JToken PrevValue;
    }
    
    public enum KeyValueOperationTypes
    {
        Add,
        Del
    }

    public class KeyValueOperation
    {
        public KeyValueOperationTypes Type;
        public string Key;
        public JToken Value;
    }

    public class KeyValueStateMachine : IRaftStateMachine
    {
        private StorageEnvironment _storageEnvironment;
        private long _lastAppliedIndex;

        public KeyValueStateMachine(StorageEnvironmentOptions options)
        {
            options.IncrementalBackupEnabled = true;
            _storageEnvironment = new StorageEnvironment(options);
            using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
            {
                _storageEnvironment.CreateTree(tx, "items");
                var metadata = _storageEnvironment.CreateTree(tx, "$metadata");
                var readResult = metadata.Read("last-index");
                if (readResult != null)
                    LastAppliedIndex = readResult.Reader.ReadLittleEndianInt64();
                tx.Commit();
            }
        }

        public event EventHandler<KeyValueOperation> OperatonExecuted;

        protected void OnOperatonExecuted(KeyValueOperation e)
        {
            var handler = OperatonExecuted;
            if (handler != null) handler(this, e);
        }

        public JToken Read(string key)
        {
            using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.Read))
            {
                var items = tx.ReadTree("items");

                var readResult = items.Read(key);

                if (readResult == null)
                    return null;


                return JToken.ReadFrom(new JsonTextReader(new StreamReader(readResult.Reader.AsStream())));
            }
        }

        public bool Cas(CasCommand op)
        {
            using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
            {
                var items = tx.ReadTree("items");
                var oldValue = items.Read(op.Key);
                var ms = new MemoryStream();
                var oldToken = oldValue == null ? JValue.CreateNull() : JToken.ReadFrom(new JsonTextReader(new StreamReader(oldValue.Reader.AsStream())));
                if (!new JTokenEqualityComparer().Equals(oldToken, op.PrevValue))
                {
                    return false;
                }

                ms.SetLength(0);

                var writer = new StreamWriter(ms);
                op.Value.WriteTo(new JsonTextWriter(writer));
                writer.Flush();

                ms.Position = 0;
                items.Add(op.Key, ms);
                tx.Commit();
                return true;
            }
        }

        public long LastAppliedIndex
        {
            get { return _lastAppliedIndex; }
            private set { Thread.VolatileWrite(ref _lastAppliedIndex, value); }
        }

        public void Apply(LogEntry entry, Command cmd)
        {
            var batch = cmd as OperationBatchCommand;
            if (batch != null)
            {
                Apply(batch.Batch, cmd.AssignedIndex);
                return;
            }
            var get = cmd as GetCommand;
            if (get != null)
            {
                cmd.CommandResult = Read(get.Key);
            }
            var cas = cmd as CasCommand;
            if (cas != null)
            {
                cmd.CommandResult = Cas(cas);

            }
            // have to apply even get command so we'll keep track of the last applied index
            Apply(Enumerable.Empty<KeyValueOperation>(), cmd.AssignedIndex);
        }

        public bool SupportSnapshots { get { return true; } }

        public void CreateSnapshot(long index, long term, ManualResetEventSlim allowFurtherModifications)
        {
            // we have not snapshot files, so this is the first time that we create a snapshot
            // we handle that by asking voron to create a full backup
            var files = Directory.GetFiles(_storageEnvironment.Options.BasePath, "*.Snapshot");
            Array.Sort(files, StringComparer.OrdinalIgnoreCase); // make sure we get it in sort order
            if (files.Any() == false)
            {
                DoFullBackup(index, term, allowFurtherModifications);
                return;
            }
            var fullBackupIndex = GetFullBackupIndex(files);
            if (fullBackupIndex == -1)
            {
                // this shouldn't be the case, we must always have at least one full backup. 
                // maybe user deleted it? We'll do a full backup here to compensate
                DoFullBackup(index, term, allowFurtherModifications);
                return;
            }
            string lastFullBackup = files[fullBackupIndex];

            var fullBackupSize = new FileInfo(lastFullBackup).Length;
            var incrementalBackupsSize = files.Skip(fullBackupIndex + 1).Sum(f => new FileInfo(f).Length);

            // now we need to decide whatever to do a full or incremental backup, doing incremental backups stop 
            // making sense if they will take more space than the full backup. Our cutoff point is when it passes to 50%
            // size of the full backup.
            // If full backup size is 1 GB, and we have 25 incrmeental backups that are 600 MB in size, we need to transfer
            // 1.6 GB to restore. If we generate a new full backup, we'll only need to transfer 1 GB to restore.

            if (incrementalBackupsSize / 2 > fullBackupSize)
            {
                DoFullBackup(index, term, allowFurtherModifications);
                return;
            }

            DeleteOldSnapshots(files.Take(fullBackupIndex - 1));// delete snapshots older than the current full backup

            var incrementalBackup = new MinimalIncrementalBackup();
            incrementalBackup.ToFile(_storageEnvironment,
                Path.Combine(_storageEnvironment.Options.BasePath, string.Format("Inc-{0:D19}-{1:D19}.Snapshot", index, term)),
                infoNotify: Console.WriteLine,
                backupStarted: allowFurtherModifications.Set);
        }

        private static int GetFullBackupIndex(string[] files)
        {
            int fullBackupIndex = -1;
            for (int i = files.Length - 1; i >= 0; i--)
            {
                if (!Path.GetFileNameWithoutExtension(files[i]).StartsWith("Full"))
                    continue;
                fullBackupIndex = i;
                break;
            }
            return fullBackupIndex;
        }

        private void DoFullBackup(long index, long term, ManualResetEventSlim allowFurtherModifications)
        {
            var snapshotsToDelete = Directory.GetFiles(_storageEnvironment.Options.BasePath, "*.Snapshot");

            var fullBackup = new FullBackup();
            fullBackup.ToFile(_storageEnvironment,
                Path.Combine(_storageEnvironment.Options.BasePath, string.Format("Full-{0:D19}-{1:D19}.Snapshot", index, term)),
                CancellationToken.None,
                infoNotify: Console.WriteLine,
                backupStarted: allowFurtherModifications.Set
                );

            DeleteOldSnapshots(snapshotsToDelete);
        }

        private static void DeleteOldSnapshots(IEnumerable<string> snapshotsToDelete)
        {
            foreach (var snapshot in snapshotsToDelete)
            {
                try
                {
                    File.Delete(snapshot);
                }
                catch (Exception)
                {
                    // we ignore snapshots we can't delete, they are expected if we are concurrently writing
                    // the snapshot and creating a new one. We'll get them the next time.
                }
            }
        }

        public ISnapshotWriter GetSnapshotWriter()
        {
            return new SnapshotWriter(this);
        }

        public class SnapshotWriter : ISnapshotWriter
        {
            private readonly KeyValueStateMachine _parent;

            private string[] files;
            private int fullBackupIndex;

            public SnapshotWriter(KeyValueStateMachine parent)
            {
                _parent = parent;
                files = Directory.GetFiles(_parent._storageEnvironment.Options.BasePath, "*.Snapshot");
                fullBackupIndex = GetFullBackupIndex(files);

                if (fullBackupIndex == -1)
                    throw new InvalidOperationException("Could not find a full backup file to start the snapshot writing");

                var last = Path.GetFileNameWithoutExtension(files[files.Length - 1]);
                Debug.Assert(last != null);
                var parts = last.Split('-');
                if (parts.Length != 3)
                    throw new InvalidOperationException("Invalid snapshot file name " + files[files.Length - 1] + ", could not figure out index & term");

                Index = long.Parse(parts[1]);
                Term = long.Parse(parts[2]);

                
            }
            
            public long Index { get; private set; }
            public long Term { get; private set; }
            public void WriteSnapshot(Stream stream)
            {
                var writer = new BinaryWriter(stream);
                writer.Write(files.Length);

                for (int i = fullBackupIndex; i < files.Length; i++)
                {
                    using (var f = File.OpenRead(files[i]))
                    {
                        writer.Write(f.Name);
                        writer.Write(f.Length);
                        writer.Flush();
                        f.CopyTo(stream);
                    }
                }
            }
        }

        public void ApplySnapshot(long term, long index, Stream stream)
        {
            var basePath = _storageEnvironment.Options.BasePath;
            _storageEnvironment.Dispose();

            foreach (var file in Directory.EnumerateFiles(basePath))
            {
                File.Delete(file);
            }

            var files = new List<string>();

            var buffer = new byte[1024 * 16];
            var reader = new BinaryReader(stream);
            var filesCount = reader.ReadInt32();
            if (filesCount == 0)
                throw new InvalidOperationException("Snapshot cannot contain zero files");
            for (int i = 0; i < filesCount; i++)
            {
                var name = reader.ReadString();
                files.Add(name);
                var len = reader.ReadInt64();
                using (var file = File.Create(Path.Combine(basePath, name)))
                {
                    file.SetLength(len);
                    var totalFileRead = 0;
                    while (totalFileRead < len)
                    {
                        var read = stream.Read(buffer, 0, (int)Math.Min(buffer.Length, len - totalFileRead));
                        if (read == 0)
                            throw new EndOfStreamException();
                        totalFileRead += read;
                        file.Write(buffer, 0, read);
                    }
                }
            }

            new FullBackup().Restore(Path.Combine(basePath, files[0]), basePath);

            var options = StorageEnvironmentOptions.ForPath(basePath);
            options.IncrementalBackupEnabled = true;

            new IncrementalBackup().Restore(options, files.Skip(1));

            _storageEnvironment = new StorageEnvironment(options);

            using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
            {
                var metadata = tx.ReadTree("$metadata");
                metadata.Add("last-index", EndianBitConverter.Little.GetBytes(index));
                LastAppliedIndex = index;
                tx.Commit();
            }
        }

        public void Danger__SetLastApplied(long postion)
        {
            LastAppliedIndex = postion;
        }

        private void Apply(IEnumerable<KeyValueOperation> ops, long commandIndex)
        {
            using (var tx = _storageEnvironment.NewTransaction(TransactionFlags.ReadWrite))
            {
                var items = tx.ReadTree("items");
                var metadata = tx.ReadTree("$metadata");
                metadata.Add("last-index", EndianBitConverter.Little.GetBytes(commandIndex));
                LastAppliedIndex = commandIndex;
                var ms = new MemoryStream();
                foreach (var op in ops)
                {
                    switch (op.Type)
                    {
                        case KeyValueOperationTypes.Add:
                            ms.SetLength(0);

                            var streamWriter = new StreamWriter(ms);
                            op.Value.WriteTo(new JsonTextWriter(streamWriter));
                            streamWriter.Flush();

                            ms.Position = 0;
                            items.Add(op.Key, ms);
                            break;
                        case KeyValueOperationTypes.Del:
                            items.Delete(op.Key);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                    OnOperatonExecuted(op);
                }

                tx.Commit();
            }
        }


        public void Dispose()
        {
            if (_storageEnvironment != null)
                _storageEnvironment.Dispose();
        }
    }
}
