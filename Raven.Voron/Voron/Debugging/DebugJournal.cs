using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

using Voron.Impl;
using Voron.Util;
using Voron.Util.Conversion;

namespace Voron.Debugging
{
    public unsafe class DebugJournal : IDisposable
    {
        public abstract class BaseActivityEntry
        {
            public int LineNumber;

            public DebugActionType ActionType { get; protected set; }

            public abstract string ToCsvLine(bool recordOnlyValueLength);

            public static BaseActivityEntry FromCsvLine(string csvLine, bool recordOnlyValueLength)
            {
                var firstToken = csvLine.Substring(0, csvLine.IndexOf(",", StringComparison.Ordinal));
                if (firstToken.StartsWith("Flush"))
                {
                    return FlushActivityEntry.FromCsvLine(csvLine);
                }
                if (firstToken.StartsWith("Transaction"))
                {
                    return TransactionActivityEntry.FromCsvLine(csvLine, recordOnlyValueLength);
                }
                return WriteActivityEntry.FromCsvLine(csvLine, recordOnlyValueLength);
            }
        }

        public abstract class TransactionAwareActivityEntry : BaseActivityEntry
        {
            public long? TransactionId { get; protected set; }
        }

        public class FlushActivityEntry : TransactionAwareActivityEntry
        {
            public FlushActivityEntry(DebugActionType actionType, long? transactionId)
            {
                ActionType = actionType;
                TransactionId = transactionId;
            }

            public override string ToCsvLine(bool recordOnlyValueLength)
            {
                return string.Format("{0},{1}", ActionType, TransactionId);
            }

            public static BaseActivityEntry FromCsvLine(string csvLine)
            {
                var columnArray = csvLine.Split(new[] { ',' }).ToList();
                if (columnArray.Count != 2)
                {
                    throw new ArgumentException("invalid csv data - check that you do not have commas in data");
                }

                try
                {
                    var actionType = GenericUtil.ParseEnum<DebugActionType>(columnArray[0]);
                    var transactionId = string.IsNullOrEmpty(columnArray[1]) ? null : (long?)long.Parse(columnArray[1]);
                    return new FlushActivityEntry(actionType, transactionId);
                }
                catch (Exception e)
                {
                    throw new ArgumentException("Unable to parse the argument", e);
                }
            }
        }

        public class TransactionActivityEntry : TransactionAwareActivityEntry
        {
            public TransactionFlags Flags { get; private set; }

            public bool CreatedByJournalApplicator { get; private set; }

            public TransactionActivityEntry(Transaction tx, DebugActionType actionType)
            {
                TransactionId = tx.Id;
                Flags = tx.Flags;
                ActionType = actionType;
                CreatedByJournalApplicator = tx.CreatedByJournalApplicator;
            }

            public TransactionActivityEntry(long transactionId, TransactionFlags flags, DebugActionType actionType, bool createdByJournalApplicator)
            {
                TransactionId = transactionId;
                Flags = flags;
                ActionType = actionType;
                CreatedByJournalApplicator = createdByJournalApplicator;
            }

            public override string ToCsvLine(bool recordOnlyValueLength)
            {
                return string.Format("{0},{1},{2},{3}", ActionType, TransactionId, Flags, CreatedByJournalApplicator);
            }

            public new static TransactionActivityEntry FromCsvLine(string csvLine, bool recordOnlyValueLength)
            {
                var columnArray = csvLine.Split(new[] { ',' }).ToList();
                if (columnArray.Count != 4)
                {
                    throw new ArgumentException("invalid csv data - check that you do not have commas in data");
                }

                try
                {
                    var actionType = GenericUtil.ParseEnum<DebugActionType>(columnArray[0]);
                    var transactionId = long.Parse(columnArray[1]);
                    var flags = GenericUtil.ParseEnum<TransactionFlags>(columnArray[2]);
                    var startedByFlusher = bool.Parse(columnArray[3]);
                    return new TransactionActivityEntry(transactionId, flags, actionType, startedByFlusher);
                }
                catch (Exception e)
                {
                    throw new ArgumentException("Unable to parse the argument", e);
                }
            }
        }

        public class WriteActivityEntry : TransactionAwareActivityEntry
        {
            public string TreeName { get; private set; }

            public Slice Key { get; private set; }

            public object Value { get; private set; }

            public Stream ValueStream
            {
                get
                {
                    if (Value == null)
                        return null;
                    if (Value is Slice)
                        return new MemoryStream(Encoding.UTF8.GetBytes(Value.ToString()));
                    if (Value is Stream)
                        return Value as Stream;
                    throw new InvalidOperationException("Value of activity activityEntry is of unsupported type");
                }
            }

            public WriteActivityEntry(DebugActionType actionType, long transactionId, Slice key, string treeName, object value)
            {
                ActionType = actionType;
                TransactionId = transactionId;
                Key = key;
                TreeName = treeName;
                Value = value;
            }

            public override string ToCsvLine(bool recordOnlyValueLength)
            {
                if (recordOnlyValueLength)
                    return ToCsvWithValueLengthOnly();

                return ToCsv();
            }

            private string ToCsv()
            {
                var entryValue = new byte[0];

                if (Value is Stream && Value != Stream.Null)
                {
                    var stream = (Stream)Value;
                    var ownsStream = false;
                    stream.Position = 0;

                    var ms = stream as MemoryStream;
                    if (ms == null)
                    {
                        ms = new MemoryStream();
                        stream.CopyTo(ms);
                        stream.Position = 0;
                        ownsStream = true;
                    }

                    entryValue = ms.ToArray();

                    if (ownsStream)
                    {
                        ms.Dispose();
                    }
                }
                else if (Value is Slice)
                {
                    var value = Value as Slice;

                    var slice = value;
                    var array = new byte[slice.Size];
                    slice.CopyTo(array);

                    entryValue = array;
                }
                else if (Value != null && (Value.GetType().IsPrimitive || Value is String))
                {
                    entryValue = Encoding.UTF8.GetBytes(Value.ToString());
                }
                else if (Value is IStructure)
                {
                    var structure = (IStructure) Value;

                    var structBytes = new byte[structure.GetSize()];

                    fixed (byte* p = structBytes)
                        structure.Write(p);

                    entryValue = structBytes;
                }
                else if (Value == Stream.Null || Value == null)
                {
                    // do nothing
                }
                else
                {
                    throw new NotSupportedException(string.Format("Given value type is not supported ({0}).", Value.GetType()));
                }

                var line = string.Format("{0},{1},{2},\"{3}\",{4}", ActionType, TransactionId, TreeName, Key, Convert.ToBase64String(entryValue));
                
                return line;
            }

            private string ToCsvWithValueLengthOnly()
            {
                long? length;
                if (Value is Stream && Value != Stream.Null)
                    length = ((Stream)Value).Length;
                else if (Value is IStructure)
                    length = ((IStructure) Value).GetSize();
                else return ToCsv();

                return string.Format("{0},{1},{2},{3},{4}", ActionType, TransactionId, TreeName, Key, length);
            }

            public new static WriteActivityEntry FromCsvLine(string csvLine, bool recordOnlyValueLength)
            {
                var columnArray = csvLine.Split(new[] { ',' }).ToList();
                if (columnArray.Count != 5)
                    throw new ArgumentException("invalid csv data - check that you do not have commas in data");

                try
                {
                    var type = GenericUtil.ParseEnum<DebugActionType>(columnArray[0]);

                    if (type == DebugActionType.CreateTree)
                    {
                        var activityEntry = new WriteActivityEntry(
                            type,
                            long.Parse(columnArray[1]),
                            Slice.Empty,
                            columnArray[2].Trim('"'),
                            null);
                        return activityEntry;
                    }

                    if (type == DebugActionType.Delete)
                    {
                        var activityEntry = new WriteActivityEntry(
                            type,
                            long.Parse(columnArray[1]),
                            (Slice)columnArray[3].Trim('"'),
                            columnArray[2].Trim('"'),
                            null);

                        return activityEntry;
                    }

                    var random = new Random();
                    
                    object value;
                    switch (type)
                    {
                        case DebugActionType.Increment:
                            var delta = long.Parse(Encoding.UTF8.GetString(Convert.FromBase64String(columnArray[4])));
                            value = new MemoryStream(EndianBitConverter.Little.GetBytes(delta));
                            break;
                        case DebugActionType.MultiAdd:
                        case DebugActionType.MultiDelete:
                               value = new Slice(Convert.FromBase64String(columnArray[4]));
                            break;
                        default:
                            if (recordOnlyValueLength)
                            {
                                var length = long.Parse(columnArray[4]);
                                var bytes = new byte[length];
                                random.NextBytes(bytes);

                                value = new MemoryStream(bytes);
                            }
                            else
                                value = new MemoryStream(Convert.FromBase64String(columnArray[4]));
                            break;
                    }

                    var entry = new WriteActivityEntry(
                        type,
                        long.Parse(columnArray[1]),
                        (Slice)columnArray[3].Trim('"'),
                        columnArray[2].Trim('"'),
                        value);

                    return entry;
                }
                catch (Exception e)
                {
                    throw new ArgumentException("Unable to parse the argument", e);
                }
            }
        }

        private StorageEnvironment _env;
        private FileStream _journalFileStream;
        private TextWriter _journalWriter;
        private const string FileExtension = ".djrs";
        private readonly object _journalWriteSyncObject = new object();
        private bool _isDisposed;
        public bool IsRecording { get; set; }

        public bool RecordOnlyValueLength { get; set; }

        public ConcurrentQueue<BaseActivityEntry> WriteQueue { get; private set; }

        public DebugJournal(string journalName, StorageEnvironment env, bool isRecordingByDefault = false)
        {
            _env = env;
            
            IsRecording = isRecordingByDefault;
            InitializeDebugJournal(journalName);
            _isDisposed = false;
        }

        private void InitializeDebugJournal(string journalName)
        {
            Dispose();

            _journalFileStream = new FileStream(journalName + FileExtension, FileMode.OpenOrCreate, FileAccess.ReadWrite);
            _journalWriter = new StreamWriter(_journalFileStream, Encoding.UTF8);
            WriteQueue = new ConcurrentQueue<BaseActivityEntry>();
        }

        public void Replay(int writeFrequency = 10000, Action<BaseActivityEntry> validate = null)
        {
            Transaction currentWriteTransaction = null;
            var lineNumber = 1;

            try
            {
                using (var journalReader = new StreamReader(_journalFileStream, Encoding.UTF8))
                {
                    while (journalReader.Peek() >= 0)
                    {
                        var csvLine = journalReader.ReadLine();
                        if (!string.IsNullOrWhiteSpace(csvLine))
                        {
                            var activityEntry = BaseActivityEntry.FromCsvLine(csvLine, RecordOnlyValueLength);
                            activityEntry.LineNumber = lineNumber;

                            WriteQueue.Enqueue(activityEntry);
                        }

                        lineNumber++;

                        if (lineNumber % writeFrequency == 0)
                        {
                            ApplyRecordedActivities(ref currentWriteTransaction, validate);
                        }
                    }
                }

                ApplyRecordedActivities(ref currentWriteTransaction, validate);
            }
            finally
            {
                if (currentWriteTransaction != null)
                    currentWriteTransaction.Dispose();
            }
        }

        public static DebugJournal FromFile(string journalName, StorageEnvironment env, bool onlyValueLength = false)
        {
            var newJournal = new DebugJournal(journalName, env)
                             {
                                 RecordOnlyValueLength = onlyValueLength
                             };

            return newJournal;
        }

        private void WriteAndFlush(TransactionAwareActivityEntry activityEntry)
        {
            lock (_journalWriteSyncObject)
                if (!_isDisposed)
                {
                    _journalWriter.WriteLine(activityEntry.ToCsvLine(RecordOnlyValueLength));
                    _journalWriter.Flush();
                }
        }

        public void RecordFlushAction(DebugActionType actionType, Transaction tx)
        {
            if (IsRecording)
            {
                var action = new FlushActivityEntry(actionType, tx != null ? (long?)tx.Id : null);
                WriteQueue.Enqueue(action);
                WriteAndFlush(action);
            }
        }

        public void RecordWriteAction(DebugActionType actionType, Transaction tx, Slice key, string treeName, object value)
        {
            if (IsRecording)
            {
                var newAction = new WriteActivityEntry(actionType, tx.Id, key, treeName, value);
                WriteQueue.Enqueue(newAction);
                WriteAndFlush(newAction);
            }
        }

        public void RecordTransactionAction(Transaction tx, DebugActionType actionType)
        {
            if (IsRecording)
            {
                var txAction = new TransactionActivityEntry(tx, actionType);
                WriteQueue.Enqueue(txAction);
                WriteAndFlush(txAction);
            }
        }

        public void Flush()
        {
            try
            {
                lock(_journalWriteSyncObject)
                    if(!_isDisposed)
                        _journalWriter.Flush();
            }
            catch (ObjectDisposedException)
            {
            }
        }

        private void ApplyRecordedActivities(ref Transaction currentWriteTransaction, Action<BaseActivityEntry> validate = null)
        {
            var wasDebugRecording = _env.IsDebugRecording;
            _env.IsDebugRecording = false;

            var readTransactions = new Dictionary<long, Queue<Transaction>>();

            BaseActivityEntry activityEntry;
            while (WriteQueue.TryDequeue(out activityEntry))
            {
                var transactionEntry = activityEntry as TransactionActivityEntry;
                if (transactionEntry != null)
                {
                    ReplayTransactionEntry(transactionEntry, ref currentWriteTransaction, readTransactions);

                    if (validate != null)
                        validate(transactionEntry);
                    continue;
                }

                var actionEntry = activityEntry as WriteActivityEntry;
                if (actionEntry != null)
                {
                    if (currentWriteTransaction != null)
                    {
                        ReplayWriteAction(actionEntry, ref currentWriteTransaction);
                        if (validate != null)
                            validate(actionEntry);
                    }
                    continue;
                }

                var flushEntry = activityEntry as FlushActivityEntry;
                if (flushEntry != null)
                {
                    ReplayFlushAction(flushEntry, currentWriteTransaction);
                    if (validate != null)
                        validate(flushEntry);
                    continue;
                }
                throw new InvalidOperationException("unsupported tree action type: " + activityEntry);
            }

            foreach (var transactions in readTransactions.Values)
            {
                foreach (var t in transactions)
                {
                    t.Dispose();
                }
            }

            _env.IsDebugRecording = wasDebugRecording; //restore the state as it was
        }

        private void ReplayFlushAction(FlushActivityEntry flushEntry, Transaction currentWriteTransaction)
        {
            if (flushEntry.ActionType == DebugActionType.FlushStart)
            {
                using (_env.Options.AllowManualFlushing())
                {
                    _env.FlushLogToDataFile(currentWriteTransaction);
                }    
            }
        }

        private void ReplayTransactionEntry(TransactionActivityEntry transactionAwareActivityEntry, ref Transaction currentWriteTransaction, Dictionary<long, Queue<Transaction>> readTransactions)
        {
            var isReadTx = transactionAwareActivityEntry.Flags == TransactionFlags.Read;
            if (isReadTx)
            {
                ReplayReadTransaction(transactionAwareActivityEntry, readTransactions);
            }
            else
            {
                ReplayWriteTransaction(transactionAwareActivityEntry, ref currentWriteTransaction);
            }
        }

        private void ReplayWriteTransaction(TransactionActivityEntry transactionAwareActivityEntry, ref Transaction currentWriteTransaction)
        {
            Transaction tx;
            switch (transactionAwareActivityEntry.ActionType)
            {
                case DebugActionType.TransactionStart:
                    tx = _env.NewTransaction(transactionAwareActivityEntry.Flags);
                    currentWriteTransaction = tx;
                    break;
                case DebugActionType.TransactionCommit:
                    currentWriteTransaction.Commit();
                    break;
                case DebugActionType.TransactionRollback:
                    currentWriteTransaction.Rollback();
                    break;
                case DebugActionType.TransactionDisposing:
                    currentWriteTransaction.Dispose();
                    currentWriteTransaction = null;
                    break;
                default:
                    throw new InvalidOperationException("unsupported action type for readWrite transaction: " + transactionAwareActivityEntry.ActionType);
            }
        }

        private void ReplayReadTransaction(TransactionActivityEntry transactionTransactionAwareActivityEntry, Dictionary<long, Queue<Transaction>> readTransactions)
        {
            var txId = transactionTransactionAwareActivityEntry.TransactionId.Value;
            Queue<Transaction> transactionGroup;

            switch (transactionTransactionAwareActivityEntry.ActionType)
            {
                case DebugActionType.TransactionStart:
                    var tx = _env.NewTransaction(transactionTransactionAwareActivityEntry.Flags);

                    if (readTransactions.TryGetValue(txId, out transactionGroup))
                    {
                        transactionGroup.Enqueue(tx);
                    }
                    else
                    {
                        var queue = new Queue<Transaction>();
                        queue.Enqueue(tx);
                        readTransactions[txId] = queue;
                    }
                    break;
                case DebugActionType.TransactionDisposing:
                    if (readTransactions.TryGetValue(txId, out transactionGroup) && transactionGroup.Count > 0)
                    {
                        var txAboutToDestroy = transactionGroup.Dequeue();
                        txAboutToDestroy.Dispose();
                        if (transactionGroup.Count == 0)
                        {
                            readTransactions.Remove(txId);
                        }
                    }
                    break;
                default:
                    throw new InvalidOperationException("unsupported action type for readOnly transaction: " + transactionTransactionAwareActivityEntry.ActionType);
            }
        }

        private void ReplayWriteAction(WriteActivityEntry activityEntry, ref Transaction tx)
        {
            var tree = tx.ReadTree(activityEntry.TreeName);

            switch (activityEntry.ActionType)
            {
                case DebugActionType.Add:
                    tree.Add(activityEntry.Key, activityEntry.ValueStream);
                    break;
                case DebugActionType.Delete:
                    tree.Delete(activityEntry.Key);
                    break;
                case DebugActionType.MultiAdd:
                    tree.MultiAdd(activityEntry.Key, new Slice(Encoding.UTF8.GetBytes(activityEntry.Value.ToString())));
                    break;
                case DebugActionType.MultiDelete:
                    tree.MultiDelete(activityEntry.Key, new Slice(Encoding.UTF8.GetBytes(activityEntry.Value.ToString())));
                    break;
                case DebugActionType.CreateTree:
                    _env.CreateTree(tx, activityEntry.TreeName);
                    break;
                case DebugActionType.Increment:
                    var buffer = new byte[sizeof(long)];
                    activityEntry.ValueStream.Read(buffer, 0, buffer.Length);
                    var delta = EndianBitConverter.Little.ToInt64(buffer, 0);
                    tree.Increment(activityEntry.Key, delta);
                    break;
                case DebugActionType.AddStruct:
                    tree.Add(activityEntry.Key, activityEntry.ValueStream);
                    break;
                case DebugActionType.RenameTree:
                    _env.RenameTree(tx, activityEntry.TreeName, activityEntry.Key.ToString());
                    break;
                default: //precaution against newly added action types
                    throw new InvalidOperationException("unsupported tree action type: " + activityEntry.ActionType);
            }
        }

        public void Dispose()
        {
            try
            {
                lock (_journalWriteSyncObject)
                {
                    if (_journalWriter != null)
                        _journalWriter.Dispose();

                    if (_journalFileStream != null)
                        _journalFileStream.Dispose();

                    _isDisposed = true;
                }
            }
            catch (ObjectDisposedException)
            {
            }
        }
    }
}
