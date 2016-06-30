using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
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
        [ThreadStatic] private static string _currentThreadId;

        private readonly ManualResetEventSlim _hasEntries = new ManualResetEventSlim(false);
        private readonly ThreadLocal<LocalThreadWriterState> _localState;
        private readonly Thread _loggingThread;

        private readonly ConcurrentQueue<WeakReference<LocalThreadWriterState>> _newThreadStates =
            new ConcurrentQueue<WeakReference<LocalThreadWriterState>>();

        private readonly string _path;
        private readonly TimeSpan _retentionTime;
        private string _dateString;
        private volatile bool _keepLogging = true;
        private int _logNumber;
        private DateTime _today;
        public bool IsInfoEnabled;
        public bool IsOperationsEnabled;

        public LoggerSetup(string path, LogMode logMode = LogMode.Information, TimeSpan retentionTime = default(TimeSpan))
        {
            _path = path;
            if (retentionTime == default(TimeSpan))
                retentionTime = TimeSpan.FromDays(3);

            _retentionTime = retentionTime;
            _localState = new ThreadLocal<LocalThreadWriterState>(GenerateThreadWriterState);
            _loggingThread = new Thread(BackgroundLogger)
            {
                IsBackground = true,
                Name = "Logging Thread"
            };
            SetupLogMode(logMode);
        }

        public void Dispose()
        {
            _keepLogging = false;
            _hasEntries.Dispose();
            if ((_loggingThread.ThreadState & ThreadState.Unstarted) != ThreadState.Unstarted)
                _loggingThread.Join();
        }

        public void SetupLogMode(LogMode logMode)
        {
            IsInfoEnabled = (logMode & LogMode.Information) == LogMode.Information;
            IsOperationsEnabled = (logMode & LogMode.Operations) == LogMode.Operations;

            if (logMode == LogMode.None)
                return;

            Directory.CreateDirectory(_path);
            if ((_loggingThread.ThreadState & ThreadState.Unstarted) == ThreadState.Unstarted)
                _loggingThread.Start();
        }


        private Stream GetNewStream()
        {
            if (DateTime.Today != _today)
            {
                lock (typeof (LoggerSetup))
                {
                    if (DateTime.Today != _today)
                    {
                        _today = DateTime.Today;
                        _dateString = DateTime.Today.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                        _logNumber = 0;

                        CleanupOldLogFiles();
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
                // TODO: If avialable file size on the disk is too small, emit a warning, and return a Null Stream, instead
                // TODO: We don't want to have the debug log kill us
                return new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.Read, 32*1024, false);
            }
        }

        private void CleanupOldLogFiles()
        {
            string[] existingLogFiles;
            try
            {
                // we use GetFiles because we don't expect to have a massive amount of files, and it is 
                // not sure what kind of iteration we get if we run and modify using Enumerate
                existingLogFiles = Directory.GetFiles(_path, "*.log"); 
            }
            catch (Exception)
            {
                return;// this can fail for various reasons, we don't really care for that in most cases
            }
            foreach (var existingLogFile in existingLogFiles)
            {
                try
                {
                    if (_today - File.GetLastWriteTimeUtc(existingLogFile) > _retentionTime)
                    {
                        File.Delete(existingLogFile);
                    }
                }
                catch (Exception)
                {
                    // we don't actually care if we can't handle this scenario, we'll just try again later
                    // maybe something is currently reading the file?
                }
            }
        }

        private LocalThreadWriterState GenerateThreadWriterState()
        {
            var state = new LocalThreadWriterState();
            _newThreadStates.Enqueue(new WeakReference<LocalThreadWriterState>(state));
            return state;
        }

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
            state.Full.Enqueue(state.ForwardingStream.Destination, timeout: 128);
            _hasEntries.Set();
        }

        private void WriteEntryToWriter(StreamWriter writer, LogEntry entry)
        {
            if (_currentThreadId == null)
            {
                _currentThreadId = ", " + Thread.CurrentThread.ManagedThreadId.ToString(CultureInfo.InvariantCulture) +
                                  ", ";
            }

            writer.Write(entry.At.GetDefaultRavenFormat(true));
            writer.Write(_currentThreadId);

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
            return GetLogger(source, typeof (T).FullName);
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
                        const int maxFileSize = 1024*1024*256;
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