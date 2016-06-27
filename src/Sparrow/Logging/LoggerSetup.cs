using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using Raven.Abstractions.Extensions;

namespace Sparrow.Logging
{
    /// <summary>
    ///     **This code was written under the influence**, and should be reviewed as such.
    ///     It is intended to be high performance logging framework, but it should be reviewed
    ///     for missing log entries, etc.
    /// </summary>
    public class LoggerSetup : IDisposable
    {
        private readonly ManualResetEventSlim _hasEntries = new ManualResetEventSlim(false);
        private readonly ThreadLocal<LocalThreadWriterState> _localState;
        private readonly Thread _loggingThread;

        private readonly ConcurrentQueue<WeakReference<LocalThreadWriterState>> _newThreadStates =
            new ConcurrentQueue<WeakReference<LocalThreadWriterState>>();

        private readonly string _path;
        private string _dateString;
        private volatile bool _keepLogging = true;
        private int _logNumber;
        private DateTime _today;
        public bool IsOperationsEnabled;
        public bool IsInfoEnabled;

        public LoggerSetup(string path, LogMode logMode = LogMode.Information)
        {
            _path = path;
            Directory.CreateDirectory(_path);
            SetupLogMode(logMode);
            _localState = new ThreadLocal<LocalThreadWriterState>(GenerateThreadWriterState);
            _loggingThread = new Thread(BackgroundLogger)
            {
                IsBackground = true,
                Name = "Logging Thread"
            };
            _loggingThread.Start();
        }

        public void SetupLogMode(LogMode logMode)
        {
            IsInfoEnabled = (logMode & LogMode.Information) == LogMode.Information;
            IsOperationsEnabled = (logMode & LogMode.Operations) == LogMode.Operations;
        }

        public void Dispose()
        {
            _keepLogging = false;
            _hasEntries.Dispose();
            _loggingThread.Join();
        }


        private Stream GetNewStream()
        {
            if (DateTime.Today != _today)
            {
                lock (typeof(LoggerSetup))
                {
                    if (DateTime.Today != _today)
                    {
                        _today = DateTime.Today;
                        _dateString = DateTime.Today.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                        _logNumber = 0;
                    }
                }
            }
            while (true)
            {
                var nextLogNumber = Interlocked.Increment(ref _logNumber);
                var fileName = Path.Combine(_path, _dateString) + "." +
                               nextLogNumber.ToString("000", CultureInfo.InvariantCulture) + ".log";
                if (File.Exists(fileName))
                    continue;
                // TODO: If avialable file size is too small, emit a warning, and return a Null Stream, instead
                // TODO: We don't want to have the debug log kill us
                return new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.Read, 32 * 1024, false);
            }
        }

        private LocalThreadWriterState GenerateThreadWriterState()
        {
            var state = new LocalThreadWriterState();
            _newThreadStates.Enqueue(new WeakReference<LocalThreadWriterState>(state));
            return state;
        }

        private readonly ThreadLocal<int> _numberOfTimesWaited = new ThreadLocal<int>();

        public void Log(ref LogEntry entry)
        {
#if DEBUG
            if (entry.Type == LogMode.Information && IsInfoEnabled == false)
                throw new InvalidOperationException("Logging of info level when information is disabled");

            if (entry.Type == LogMode.Operations && IsOperationsEnabled == false)
                throw new InvalidOperationException("Logging of ops level when ops is disabled");
#endif

            var state = _localState.Value;
            if (state.Free.Dequeue(out state.ForwardingStream.Destination))
            {
                state.ForwardingStream.Destination.SetLength(0);
            }
            else
            {
                state.ForwardingStream.Destination = new MemoryStream();
            }
            WriteEntryToWriter(state.Writer, entry);
            EnqueueLogEntry(state);
            _hasEntries.Set();
        }

        private void EnqueueLogEntry(LocalThreadWriterState state)
        {
            int i = 0;
            for (; i < 128; i++)
            {
                if (state.Full.Enqueue(state.ForwardingStream.Destination))
                    break;
                _numberOfTimesWaited.Value++;
                if (_numberOfTimesWaited.Value > 1024)
                {
                    // we skipped quite a lot, let us reset the wait and see 
                    // if we can get it into the queue for fast enough processing
                    _numberOfTimesWaited.Value = 120;
                }
                else if (_numberOfTimesWaited.Value > 128)
                    return;// we'll discard this immediately, nothing to do here
                Thread.Sleep(2);
            }
            if (i < 5 && _numberOfTimesWaited.Value > 0)
            {
                _numberOfTimesWaited.Value = 0;
            }
        }

        [ThreadStatic]
        private static string CurrentThreadId;

        private void WriteEntryToWriter(StreamWriter writer, LogEntry entry)
        {
            if (CurrentThreadId == null)
            {
                CurrentThreadId = ", " + Thread.CurrentThread.ManagedThreadId.ToString(CultureInfo.InvariantCulture) + ", ";
            }

            writer.Write(entry.At.GetDefaultRavenFormat(isUtc: true));
            writer.Write(CurrentThreadId);

            switch (entry.Type)
            {
                case LogMode.Information:
                    writer.Write("Information");
                    break;
                case LogMode.Operations:
                    writer.Write("Operations");
                    break;
            }
            writer.Write(", ");
            writer.Write(entry.Source);
            writer.Write(", ");
            writer.Write(entry.Logger);
            writer.Write(", ");
            writer.Write(entry.Message);
            writer.Write(", ");
            if (entry.Exception != null)
            {
                writer.Write(entry.Exception);
            }
            writer.WriteLine();
            writer.Flush();
        }

        public Logger GetLogger<T>(string source)
        {
            return GetLogger(source, typeof(T).FullName);
        }

        public Logger GetLogger(string source, string logger)
        {
            return new Logger(this, source, logger);
        }

        private void BackgroundLogger()
        {
            try
            {
                var threadStates = new List<WeakReference<LocalThreadWriterState>>();
                while (_keepLogging)
                {
                    using (var currentFile = GetNewStream())
                    {
                        const int maxFileSize = 1024 * 1024 * 256;
                        var sizeWritten = 0;

                        var foundEntry = true;

                        while (sizeWritten < maxFileSize)
                        {
                            if (foundEntry == false)
                            {
                                if (_keepLogging == false)
                                    return;

                                _hasEntries.Wait();
                                _hasEntries.Reset();
                            }
                            foundEntry = false;
                            foreach (var threadStateWeakRef in threadStates)
                            {
                                LocalThreadWriterState threadState;
                                if (threadStateWeakRef.TryGetTarget(out threadState) == false)
                                {
                                    threadStates.Remove(threadStateWeakRef);
                                    break; // so we won't try to iterate over the mutated collection
                                }
                                for (var i = 0; i < 16; i++)
                                {
                                    MemoryStream item;
                                    if (threadState.Full.Dequeue(out item) == false)
                                        break;
                                    foundEntry = true;
                                    ArraySegment<byte> bytes;
                                    item.TryGetBuffer(out bytes);
                                    currentFile.Write(bytes.Array, bytes.Offset, bytes.Count);
                                    sizeWritten += bytes.Count;
                                    item.SetLength(0);
                                    threadState.Free.Enqueue(item);
                                }
                            }
                            if (_newThreadStates.IsEmpty == false)
                            {
                                WeakReference<LocalThreadWriterState> result;
                                while (_newThreadStates.TryDequeue(out result))
                                {
                                    threadStates.Add(result);
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                var msg = $"FATAL ERROR trying to log!{Environment.NewLine}{e}";
                Console.WriteLine(msg);
                //TODO: Log to event viewer in Windows and sys trace in Linux?
            }
        }

        private class LocalThreadWriterState
        {
            public readonly ForwardingStream ForwardingStream;

            public readonly SingleProducerSingleConsumerCircularQueue Free =
                new SingleProducerSingleConsumerCircularQueue(1024);

            public readonly SingleProducerSingleConsumerCircularQueue Full =
                new SingleProducerSingleConsumerCircularQueue(1024);

            public readonly StreamWriter Writer;

            public LocalThreadWriterState()
            {
                ForwardingStream = new ForwardingStream();
                Writer = new StreamWriter(ForwardingStream);
            }
        }


        private class ForwardingStream : Stream
        {
            public MemoryStream Destination;
            public override bool CanRead { get; } = false;
            public override bool CanSeek { get; } = false;
            public override bool CanWrite { get; } = true;

            public override long Length
            {
                get { throw new NotSupportedException(); }
            }

            public override long Position
            {
                get { throw new NotSupportedException(); }
                set { throw new NotSupportedException(); }
            }

            public override void Flush()
            {
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                Destination.Write(buffer, offset, count);
            }

            public override void SetLength(long value)
            {
                throw new NotSupportedException();
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotSupportedException();
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }
        }
    }
}