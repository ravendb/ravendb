using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Voron.Impl;
using Voron.Trees;

namespace Voron.Debugging
{
    public class DebugJournal : IDisposable
    {
        public class ActivityEntry
        {
            public DebugActionType ActionType { get; private set; }

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

                    throw new InvalidOperationException("Value of activity entry is of unsupported type");
                }
            }

            public ActivityEntry(DebugActionType actionType, Slice key, string treeName, object value)
            {
                ActionType = actionType;
                Key = key;
                TreeName = treeName;
                Value = value;
            }

            public string ToCsvLine(bool recordOnlyValueLength)
            {
                if (recordOnlyValueLength)
                    return ToCsvWithValueLengthOnly();

                return ToCsv();
            }

            private string ToCsv()
            {
                string entryValue = null;

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

                    entryValue = Encoding.UTF8.GetString(ms.ToArray());

                    if (ownsStream)
                    {
                        ms.Dispose();
                    }
                }
                else
                {
                    var value = Value as Slice;
                    if (value != null)
                    {
                        var slice = value;
                        var array = new byte[slice.Size];
                        slice.CopyTo(array);

                        entryValue = Encoding.UTF8.GetString(array);
                    }
                    else if (Value == Stream.Null || Value == null)
                    {
                        // do nothing
                    }
                    else
                    {
                        throw new NotSupportedException(string.Format("Given value type is not supported ({0}).", Value.GetType()));
                    }
                }

                return string.Format("{0},{1},{2},{3}", ActionType, TreeName, Key, entryValue);
            }

            private string ToCsvWithValueLengthOnly()
            {
                long? length;
                if (Value is Stream && Value != Stream.Null)
                    length = ((Stream)Value).Length;
                else return ToCsv();

                return string.Format("{0},{1},{2},{3}", ActionType, TreeName, Key, length);
            }

            public static ActivityEntry FromCsvLine(string csvLine, bool recordOnlyValueLength)
            {
                var columnArray = csvLine.Split(new[] { ',' }).ToList();
                if (columnArray.Count != 4)
                    throw new ArgumentException("invalid csv data - check that you do not have commas in data");

                try
                {
                    if (columnArray[0] == DebugActionType.CreateTree.ToString())
                    {
                        var activityEntry = new ActivityEntry(
                            GenericUtil.ParseEnum<DebugActionType>(columnArray[0]),
                            Slice.Empty,
                            columnArray[1],
                            null);
                        return activityEntry;
                    }

                    if (columnArray[0] == DebugActionType.Delete.ToString())
                    {
                        var activityEntry = new ActivityEntry(
                            GenericUtil.ParseEnum<DebugActionType>(columnArray[0]),
                            columnArray[2],
                            columnArray[1],
                            null);

                        return activityEntry;
                    }

                    var random = new Random();

                    var type = GenericUtil.ParseEnum<DebugActionType>(columnArray[0]);
                    object value;
                    switch (type)
                    {
                        case DebugActionType.MultiAdd:
                        case DebugActionType.MultiDelete:
                               value = new Slice(Encoding.UTF8.GetBytes(columnArray[3]));
                            break;
                        default:
                            if (recordOnlyValueLength)
                            {
                                var length = long.Parse(columnArray[3]);
                                var bytes = new byte[length];
                                random.NextBytes(bytes);

                                value = new MemoryStream(bytes);
                            }
                            else
                                value = new MemoryStream(Encoding.UTF8.GetBytes(columnArray[3]));
                            break;
                    }

                    var entry = new ActivityEntry(type,
                        columnArray[2],
                        columnArray[1],
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

        public bool IsRecording { get; set; }

        public bool RecordOnlyValueLength { get; set; }

        public ConcurrentQueue<ActivityEntry> WriteQueue { get; private set; }

        public DebugJournal(string journalName, StorageEnvironment env, bool isRecordingByDefault = false)
        {
            _env = env;
            IsRecording = isRecordingByDefault;
            InitializeDebugJournal(journalName);
        }

        private void InitializeDebugJournal(string journalName)
        {
            Dispose();
            _journalFileStream = new FileStream(journalName + FileExtension, FileMode.OpenOrCreate, FileAccess.ReadWrite);
            _journalWriter = new StreamWriter(_journalFileStream, Encoding.UTF8);
            WriteQueue = new ConcurrentQueue<ActivityEntry>();
        }

        public void Load(string journalName)
        {
            InitializeDebugJournal(journalName);
            using (var journalReader = new StreamReader(_journalFileStream, Encoding.UTF8))
            {
                while (journalReader.Peek() >= 0)
                {
                    var csvLine = journalReader.ReadLine();
                    if (!string.IsNullOrWhiteSpace(csvLine))
                        WriteQueue.Enqueue(ActivityEntry.FromCsvLine(csvLine, RecordOnlyValueLength));
                }
            }

        }


        public static DebugJournal FromFile(string journalName, StorageEnvironment env)
        {
            var newJournal = new DebugJournal(journalName, env);
            newJournal.Load(journalName);

            return newJournal;
        }

        [Conditional("DEBUG")]
        public void RecordAction(DebugActionType actionType, Slice key, string treeName, object value)
        {
            if (IsRecording)
            {
                var newAction = new ActivityEntry(actionType, key, treeName, value);
                WriteQueue.Enqueue(newAction);
                _journalWriter.WriteLine(newAction.ToCsvLine(RecordOnlyValueLength));
            }
        }

        [Conditional("DEBUG")]
        public void Flush()
        {
            try
            {
                _journalWriter.Flush();
            }
            catch (ObjectDisposedException)
            {
            }
        }

        public void Replay()
        {
            var wasDebugRecording = _env.IsDebugRecording;
            _env.IsDebugRecording = false;

            using (var writeBatch = new WriteBatch())
            {
                ActivityEntry entry;
                while (WriteQueue.TryDequeue(out entry))
                {
                    switch (entry.ActionType)
                    {
                        case DebugActionType.Add:
                            writeBatch.Add(entry.Key, entry.ValueStream, entry.TreeName);
                            break;
                        case DebugActionType.Delete:
                            writeBatch.Delete(entry.Key, entry.TreeName);
                            break;
                        case DebugActionType.MultiAdd:
                            writeBatch.MultiAdd(entry.Key, new Slice(Encoding.UTF8.GetBytes(entry.Value.ToString())), entry.TreeName);
                            break;
                        case DebugActionType.MultiDelete:
                            writeBatch.MultiDelete(entry.Key, new Slice(Encoding.UTF8.GetBytes(entry.Value.ToString())), entry.TreeName);
                            break;
                        case DebugActionType.CreateTree:
                            using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
                            {
                                _env.CreateTree(tx, entry.TreeName);
                                tx.Commit();
                            }
                            break;
                        default: //precaution against newly added action types
                            throw new InvalidOperationException("unsupported tree action type");
                    }
                }

                _env.Writer.Write(writeBatch);
            }

            _env.IsDebugRecording = wasDebugRecording; //restore the state as it was
        }

        public void Dispose()
        {
            try
            {
                if (_journalWriter != null)
                    _journalWriter.Dispose();

                if (_journalFileStream != null)
                    _journalFileStream.Dispose();
            }
            catch (ObjectDisposedException)
            {
            }
        }
    }
}
