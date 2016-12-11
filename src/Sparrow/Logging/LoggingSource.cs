using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Sparrow.Collections;

namespace Sparrow.Logging
{
    public class LoggingSource
    {
        [ThreadStatic] private static string _currentThreadId;

        private readonly ManualResetEventSlim _hasEntries = new ManualResetEventSlim(false);
        private readonly ThreadLocal<LocalThreadWriterState> _localState;
        private Thread _loggingThread;

        private readonly ConcurrentQueue<WeakReference<LocalThreadWriterState>> _newThreadStates =
            new ConcurrentQueue<WeakReference<LocalThreadWriterState>>();

        private string _path;
        private readonly TimeSpan _retentionTime;
        private string _dateString;
        private volatile bool _keepLogging = true;
        private int _logNumber;
        private DateTime _today;
        public bool IsInfoEnabled;
        public bool IsOperationsEnabled;

        public static LoggingSource Instance = new LoggingSource(Path.GetTempPath(), LogMode.None);


        private static byte[] _headerRow =
            Encoding.UTF8.GetBytes("Time,\tThread,\tLevel,\tSource,\tLogger,\tMessage,\tException");

        public class WebSocketContext
        {
            public TaskCompletionSource<object> TaskCompletion { get; } = new TaskCompletionSource<object>();
            public LogginFilter Filter { get; } = new LogginFilter();
        }
        private readonly ConcurrentDictionary<WebSocket, WebSocketContext> _listeners = new ConcurrentDictionary<WebSocket, WebSocketContext>();

        private LogMode _logMode;
        private LogMode _oldLogMode;

        public async Task<string> ReadFromWebsocket(WebSocket source, CancellationTokenSource sourceToken)
        {
            ArraySegment<byte> buffer = new ArraySegment<byte>(new byte[1024]);

            using (var ms = new MemoryStream())
            {
                WebSocketReceiveResult result;
                do
                {
                    result = await source.ReceiveAsync(buffer, sourceToken.Token);
                    if (result.CloseStatus != null)
                    {
                        sourceToken.Cancel();
                        return "";
                    }
                    ms.Write(buffer.Array, buffer.Offset, result.Count);
                }
                while (!result.EndOfMessage);

                ms.Seek(0, SeekOrigin.Begin);

                using (var reader = new StreamReader(ms, Encoding.UTF8))
                    return reader.ReadToEnd();
            }
        }

        public async Task Register(WebSocket source,string db = null)
        {
            await source.SendAsync(new ArraySegment<byte>(_headerRow), WebSocketMessageType.Text, true,
                CancellationToken.None);
            var context = new WebSocketContext();
            if (db != null)
            {
                context.Filter.Add("source:"+db);
            }
            lock (this)
            {
                if (_listeners.Count == 0)
                {
                    _oldLogMode = _logMode;
                    SetupLogMode(LogMode.Information, _path);
                }
                if (_listeners.TryAdd(source, context) == false)
                    throw new InvalidOperationException("Socket was already added?");
            }

            CancellationTokenSource tokenSource = new CancellationTokenSource();
            CancellationToken token = tokenSource.Token;

            while (!token.IsCancellationRequested)
            {
                var res = context.Filter.ParseInput(await ReadFromWebsocket(source, tokenSource));
                if (!token.IsCancellationRequested)
                {
                    await source.SendAsync(new ArraySegment<byte>(Encoding.UTF8.GetBytes(res)), WebSocketMessageType.Text, true,
            token);
                }
            }

            await context.TaskCompletion.Task;
        }


        private LoggingSource(string path, LogMode logMode = LogMode.Information, TimeSpan retentionTime = default(TimeSpan))
        {
            _path = path;
            if (retentionTime == default(TimeSpan))
                retentionTime = TimeSpan.FromDays(3);

            _retentionTime = retentionTime;
            _localState = new ThreadLocal<LocalThreadWriterState>(GenerateThreadWriterState);
            
            SetupLogMode(logMode, path);
        }

        public void SetupLogMode(LogMode logMode, string path)
        {
            lock (this)
            {
                if (_logMode == logMode)
                    return;
                _logMode = logMode;
                IsInfoEnabled = (logMode & LogMode.Information) == LogMode.Information;
                IsOperationsEnabled = (logMode & LogMode.Operations) == LogMode.Operations;

                _path = path;

                Directory.CreateDirectory(_path);
                if (_loggingThread == null)
                {
                    StartNewLoggingThread();
                }
                else
                {
                    _keepLogging = false;
                    _hasEntries.Set();
                    _loggingThread.Join();
                    StartNewLoggingThread();
                }
            }
        }

        private void StartNewLoggingThread()
        {
            if (IsInfoEnabled == false &&
                IsOperationsEnabled == false)
                return;

            _keepLogging = true;
            _loggingThread = new Thread(BackgroundLogger)
            {
                IsBackground = true,
                Name = "Logging Thread"
            };
            _loggingThread.Start();
        }


        private Stream GetNewStream(long maxFileSize)
        {
            if (DateTime.Today != _today)
            {
                lock (this)
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
                if (File.Exists(fileName) && new FileInfo(fileName).Length >= maxFileSize)
                    continue;
                // TODO: If avialable file size on the disk is too small, emit a warning, and return a Null Stream, instead
                // TODO: We don't want to have the debug log kill us
                var fileStream = new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.Read, 32*1024, false);
                fileStream.Write(_headerRow, 0, _headerRow.Length);
                return fileStream;
            }
        }

        private void CleanupOldLogFiles()
        {
            string[] existingLogFiles;
            try
            {
                // we use GetFiles because we don't expect to have a massive amount of files, and it is 
                // not sure what kind of iteration order we get if we run and modify using Enumerate
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
            MemoryStream destination;
            var state = _localState.Value;
            foreach (var kvp in _listeners)
            {
                if (!kvp.Value.Filter.Forward(entry))
                {
                    state.BlockedWebSockets.Add(kvp.Key);
                }
            }

            if (state.Free.Dequeue(out destination))
            {
                destination.SetLength(0);
                state.ForwardingStream.Destination = destination;
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
                    const int maxFileSize = 1024*1024*256;
                    using (var currentFile = GetNewStream(maxFileSize))
                    {
                        var sizeWritten = 0;

                        var foundEntry = true;

                        while (sizeWritten < maxFileSize)
                        {
                            if (foundEntry == false)
                            {
                                if (_keepLogging == false)
                                    return;

                                _hasEntries.Wait();
                                if (_keepLogging == false)
                                    return;

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
                                    if (_listeners.Count != 0)
                                        WriteToListeningWebSockets(bytes, threadState);
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

        private void WriteToListeningWebSockets(ArraySegment<byte> bytes, LocalThreadWriterState state)
        {
            foreach (var socket in _listeners.Keys)
            {
                if (state.BlockedWebSockets.Contains(socket))
                {
                    continue;
                }
                try
                {
                    socket.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None).Wait();
                }
                catch (Exception)
                {
                    WebSocketContext value;
                    if (_listeners.TryRemove(socket, out value))
                    {
                        Task.Run(() => value.TaskCompletion.TrySetResult(null));
                    }
                    if (_listeners.Count == 0)
                    {
                        lock (this)
                        {
                            if (_listeners.Count == 0)
                            {
                                SetupLogMode(_oldLogMode, _path);
                            }
                        }
                    }
                }
            }
        }

        private class LocalThreadWriterState
        {
            public readonly ForwardingStream ForwardingStream;

            public readonly List<WebSocket> BlockedWebSockets = new List<WebSocket>();

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