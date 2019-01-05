using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Binary;
using Sparrow.Collections;
using Sparrow.Extensions;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Sparrow.Logging
{
    public sealed class LoggingSource
    {
        [ThreadStatic]
        private static string _currentThreadId;

        public static bool UseUtcTime;
        internal static long LocalToUtcOffsetInTicks;

        static LoggingSource()
        {
            LocalToUtcOffsetInTicks = TimeZoneInfo.Local.GetUtcOffset(DateTime.Now).Ticks;
            ThreadLocalCleanup.ReleaseThreadLocalState += () => _currentThreadId = null;
        }

        private readonly ManualResetEventSlim _hasEntries = new ManualResetEventSlim(false);
        private readonly ThreadLocal<LocalThreadWriterState> _localState;
        private Thread _loggingThread;
        private int _generation;
        private readonly ConcurrentQueue<WeakReference<LocalThreadWriterState>> _newThreadStates =
            new ConcurrentQueue<WeakReference<LocalThreadWriterState>>();

        private bool _updateLocalTimeOffset;
        private string _path;
        private readonly string _name;
        private TimeSpan _retentionTime;
        private string _dateString;
        private readonly MultipleUseFlag _keepLogging = new MultipleUseFlag(true);
        private int _logNumber;
        private DateTime _today;
        public bool IsInfoEnabled;
        public bool IsOperationsEnabled;

        private Stream _additionalOutput;

        private Stream _pipeSink;
        private static readonly int TimeToWaitForLoggingToEndInMilliseconds = 5_000;

        public static readonly LoggingSource Instance = new LoggingSource(LogMode.None, Path.GetTempPath(), TimeSpan.FromDays(3), "Logging")
        {
            _updateLocalTimeOffset = true
        };
        public static readonly LoggingSource AuditLog = new LoggingSource(LogMode.None, Path.GetTempPath(), TimeSpan.MaxValue, "Audit Log");

        private static readonly byte[] _headerRow =
            Encodings.Utf8.GetBytes($"Time,\tThread,\tLevel,\tSource,\tLogger,\tMessage,\tException{Environment.NewLine}");

        public class WebSocketContext
        {
            public LoggingFilter Filter { get; } = new LoggingFilter();
        }

        private readonly ConcurrentDictionary<WebSocket, WebSocketContext> _listeners =
            new ConcurrentDictionary<WebSocket, WebSocketContext>();

        public LogMode LogMode { get; private set; }
        private LogMode _oldLogMode;

        public async Task Register(WebSocket source, WebSocketContext context, CancellationToken token)
        {
            await source.SendAsync(new ArraySegment<byte>(_headerRow), WebSocketMessageType.Text, true, token).ConfigureAwait(false);

            lock (this)
            {
                if (_listeners.IsEmpty)
                {
                    _oldLogMode = LogMode;
                    SetupLogMode(LogMode.Information, _path);
                }
                if (_listeners.TryAdd(source, context) == false)
                    throw new InvalidOperationException("Socket was already added?");

                Console.WriteLine("Listener added");
            }

            var arraySegment = new ArraySegment<byte>(new byte[512]);
            var buffer = new StringBuilder();
            var charBuffer = new char[Encodings.Utf8.GetMaxCharCount(arraySegment.Count)];
            while (token.IsCancellationRequested == false)
            {
                buffer.Length = 0;
                WebSocketReceiveResult result;
                do
                {
                    result = await source.ReceiveAsync(arraySegment, token).ConfigureAwait(false);
                    if (result.CloseStatus != null)
                    {
                        return;
                    }
                    var chars = Encodings.Utf8.GetChars(arraySegment.Array, 0, result.Count, charBuffer, 0);
                    buffer.Append(charBuffer, 0, chars);
                } while (!result.EndOfMessage);

                var commandResult = context.Filter.ParseInput(buffer.ToString());
                var maxBytes = Encodings.Utf8.GetMaxByteCount(commandResult.Length);
                // We take the easy way of just allocating a large buffer rather than encoding
                // in a loop since large replies here are very rare.
                if (maxBytes > arraySegment.Count)
                    arraySegment = new ArraySegment<byte>(new byte[Bits.NextPowerOf2(maxBytes)]);

                var numberOfBytes = Encodings.Utf8.GetBytes(commandResult, 0,
                    commandResult.Length,
                    arraySegment.Array,
                    0);

                await source.SendAsync(new ArraySegment<byte>(arraySegment.Array, 0, numberOfBytes),
                    WebSocketMessageType.Text, true,
                    token).ConfigureAwait(false);
            }
        }

        public LoggingSource(LogMode logMode, string path, TimeSpan retentionTime, string name)
        {
            _path = path;
            _name = name;
            _localState = new ThreadLocal<LocalThreadWriterState>(GenerateThreadWriterState);

            SetupLogMode(logMode, path, retentionTime);
        }

        public void SetupLogMode(LogMode logMode, string path, TimeSpan retentionTime = default)
        {
            lock (this)
            {
                if (LogMode == logMode && path == _path && retentionTime == _retentionTime)
                    return;
                LogMode = logMode;
                _path = path;
                _retentionTime = retentionTime == default ? TimeSpan.FromDays(3) : retentionTime;

                IsInfoEnabled = (logMode & LogMode.Information) == LogMode.Information;
                IsOperationsEnabled = (logMode & LogMode.Operations) == LogMode.Operations;

                Directory.CreateDirectory(_path);
                var copyLoggingThread = _loggingThread;
                if (copyLoggingThread == null)
                {
                    StartNewLoggingThread();
                }
                else if (copyLoggingThread.ManagedThreadId == Thread.CurrentThread.ManagedThreadId)
                {
                    // have to do this on a separate thread
                    Task.Run(() =>
                    {
                        _keepLogging.Lower();
                        _hasEntries.Set();

                        copyLoggingThread.Join();
                        StartNewLoggingThread();
                    });
                }
                else
                {
                    _keepLogging.Lower();
                    _hasEntries.Set();

                    copyLoggingThread.Join();
                    StartNewLoggingThread();
                }
            }
        }

        private void StartNewLoggingThread()
        {
            if (IsInfoEnabled == false &&
                IsOperationsEnabled == false)
                return;

            _keepLogging.Raise();
            _loggingThread = new Thread(BackgroundLogger)
            {
                IsBackground = true,
                Name = _name + " Thread"
            };
            _loggingThread.Start();
        }

        public void EndLogging()
        {
            _keepLogging.Lower();
            _hasEntries.Set();
            _loggingThread.Join(TimeToWaitForLoggingToEndInMilliseconds);

        }

        private FileStream GetNewStream(long maxFileSize)
        {
            if (DateTime.Today != _today)
            {
                lock (this)
                {
                    if (DateTime.Today != _today)
                    {
                        _today = DateTime.Today;
                        _dateString = DateTime.Today.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                        _logNumber = GetNextLogNumberForToday();
                        CleanupOldLogFiles();
                    }
                }
            }

            UpdateLocalDateTimeOffset();

            while (true)
            {
                var nextLogNumber = Interlocked.Increment(ref _logNumber);
                var fileName = Path.Combine(_path, _dateString) + "." +
                               nextLogNumber.ToString("000", CultureInfo.InvariantCulture) + ".log";
                if (File.Exists(fileName) && new FileInfo(fileName).Length >= maxFileSize)
                    continue;
                var fileStream = SafeFileStream.Create(fileName, FileMode.Append, FileAccess.Write, FileShare.Read, 32 * 1024, false);
                fileStream.Write(_headerRow, 0, _headerRow.Length);
                return fileStream;
            }
        }

        private void UpdateLocalDateTimeOffset()
        {
            if (_updateLocalTimeOffset == false || UseUtcTime)
                return;

            var offset = TimeZoneInfo.Local.GetUtcOffset(DateTime.Now).Ticks;
            if (offset != LocalToUtcOffsetInTicks)
                Interlocked.Exchange(ref LocalToUtcOffsetInTicks, offset);
        }

        private int GetNextLogNumberForToday()
        {
            var lastLogFile = Directory.GetFiles(_path, $"{_dateString}.*.log").LastOrDefault();
            if (lastLogFile == null)
                return 0;

            int start = lastLogFile.LastIndexOf('.', lastLogFile.Length - "000.log".Length);
            if (start == -1)
                return 0;

            try
            {
                start++;
                var length = lastLogFile.Length - ".log".Length - start;
                var logNumber = lastLogFile.Substring(start, length);
                if (int.TryParse(logNumber, out var number) == false ||
                    number <= 0)
                    return 0;

                return --number;
            }
            catch
            {
                return 0;
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
                return; // this can fail for various reasons, we don't really care for that in most cases
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
            var currentThread = Thread.CurrentThread;
            var state = new LocalThreadWriterState
            {
                OwnerThread = currentThread.Name,
                ThreadId = currentThread.ManagedThreadId,
                Generation = _generation
            };
            _newThreadStates.Enqueue(new WeakReference<LocalThreadWriterState>(state));
            return state;
        }

        public void Log(ref LogEntry entry, TaskCompletionSource<object> tcs = null, bool track = false)
        {
            var state = _localState.Value;
            if (state.Generation != _generation)
            {
                state = _localState.Value = GenerateThreadWriterState();
            }

            if (state.Free.Dequeue(out var item))
            {
                item.Track = false;
                item.Data.SetLength(0);
                item.WebSocketsList.Clear();
                item.Task = tcs;
                state.ForwardingStream.Destination = item.Data;
            }
            else
            {
                item = new WebSocketMessageEntry();
                item.Task = tcs;
                state.ForwardingStream.Destination = new MemoryStream();
            }

            item.Track = track;

            foreach (var kvp in _listeners)
            {
                if (kvp.Value.Filter.Forward(ref entry))
                {
                    item.WebSocketsList.Add(kvp.Key);
                }
            }

            if (item.Track)
                Console.WriteLine($"Sockets added: {item.WebSocketsList.Count}");

            WriteEntryToWriter(state.Writer, ref entry);
            item.Data = state.ForwardingStream.Destination;

            var enqueued = state.Full.Enqueue(item, timeout: 128);

            if (item.Track)
                Console.WriteLine("Log enqueued: " + enqueued);

            _hasEntries.Set();
        }

        private void WriteEntryToWriter(StreamWriter writer, ref LogEntry entry)
        {
            if (_currentThreadId == null)
            {
                _currentThreadId = ", " + Thread.CurrentThread.ManagedThreadId.ToString(CultureInfo.InvariantCulture) +
                                   ", ";
            }

            writer.Write(entry.At.GetDefaultRavenFormat(isUtc: LoggingSource.UseUtcTime));
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

            if (entry.Exception != null)
            {
                writer.Write(", EXCEPTION: ");
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
            NativeMemory.EnsureRegistered();
            try
            {
                Interlocked.Increment(ref _generation);
                var threadStates = new List<WeakReference<LocalThreadWriterState>>();
                var threadStatesToRemove = new FastStack<WeakReference<LocalThreadWriterState>>();
                while (_keepLogging)
                {
                    try
                    {
                        const int maxFileSize = 1024 * 1024 * 256;
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
                                    // we don't want to have fsync here, we just
                                    // want to send it to the OS
                                    currentFile.Flush(flushToDisk: false);
                                    if (_hasEntries.IsSet == false)
                                        // about to go to sleep, so can check if need to update offset
                                        UpdateLocalDateTimeOffset();
                                    _hasEntries.Wait();
                                    if (_keepLogging == false)
                                        return;

                                    _hasEntries.Reset();
                                }

                                foundEntry = false;
                                foreach (var threadStateRef in threadStates)
                                {
                                    if (threadStateRef.TryGetTarget(out LocalThreadWriterState threadState) == false)
                                    {
                                        threadStatesToRemove.Push(threadStateRef);
                                        continue;
                                    }

                                    for (var i = 0; i < 16; i++)
                                    {
                                        if (threadState.Full.Dequeue(out WebSocketMessageEntry item) == false)
                                            break;

                                        foundEntry = true;

                                        sizeWritten += ActualWriteToLogTargets(item, currentFile);

                                        threadState.Free.Enqueue(item);
                                    }
                                }

                                while (threadStatesToRemove.TryPop(out var ts))
                                    threadStates.Remove(ts);

                                if (_newThreadStates.IsEmpty)
                                    continue;

                                while (_newThreadStates.TryDequeue(out WeakReference<LocalThreadWriterState> result))
                                    threadStates.Add(result);

                                _hasEntries.Set(); // we need to start writing logs again from new thread states
                            }
                        }
                    }
                    catch (OutOfMemoryException)
                    {
                        Console.Error.WriteLine("ERROR! Out of memory exception while trying to log, will avoid logging for the next 5 seconds");

                        var time = 5000;
                        var current = Stopwatch.GetTimestamp();

                        while (time > 0 &&
                            _hasEntries.Wait(time))
                        {
                            _hasEntries.Reset();
                            time = (int)(((Stopwatch.GetTimestamp() - current) * Stopwatch.Frequency) / 1000);
                            foreach (var threadStateRef in threadStates)
                            {
                                DiscardThreadLogState(threadStateRef);
                            }
                            foreach (var newThreadState in _newThreadStates)
                            {
                                DiscardThreadLogState(newThreadState);
                            }
                            current = Stopwatch.GetTimestamp();
                        }
                    }
                }
            }
            catch (Exception e)
            {
                var msg = $"FATAL ERROR trying to log!{Environment.NewLine}{e}";
                Console.Error.WriteLine(msg);
            }
        }

        private static void DiscardThreadLogState(WeakReference<LocalThreadWriterState> threadStateRef)
        {
            if (threadStateRef.TryGetTarget(out LocalThreadWriterState threadState) == false)
                return;
            while (threadState.Full.Dequeue(out WebSocketMessageEntry _))
                break;
        }

        public void AttachPipeSink(Stream stream)
        {
            _pipeSink = stream;
        }

        public void DetachPipeSink()
        {
            _pipeSink = null;
        }

        private int ActualWriteToLogTargets(WebSocketMessageEntry item, Stream file)
        {
            if (item.Track)
                Console.WriteLine("Writing to targets");

            item.Data.TryGetBuffer(out var bytes);
            file.Write(bytes.Array, bytes.Offset, bytes.Count);
            _additionalOutput?.Write(bytes.Array, bytes.Offset, bytes.Count);

            if (item.Task != null)
            {
                try
                {
                    file.Flush();
                    _additionalOutput?.Flush();
                }
                finally
                {
                    item.Task.TrySetResult(null);
                }
            }

            try
            {
                _pipeSink?.Write(bytes.Array, bytes.Offset, bytes.Count);
            }
            catch
            {
                // broken pipe
            }

            if (!_listeners.IsEmpty)
            {
                // this is rare
                SendToWebSockets(item, bytes);
            }

            item.Data.SetLength(0);
            item.WebSocketsList.Clear();

            return bytes.Count;
        }

        private Task[] _tasks = new Task[0];

        private void SendToWebSockets(WebSocketMessageEntry item, ArraySegment<byte> bytes)
        {
            if (_tasks.Length != item.WebSocketsList.Count)
                Array.Resize(ref _tasks, item.WebSocketsList.Count);

            if (item.Track)
                Console.WriteLine("Sending...");

            for (int i = 0; i < item.WebSocketsList.Count; i++)
            {
                var socket = item.WebSocketsList[i];
                try
                {
                    _tasks[i] = socket.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Send error: {e}");

                    RemoveWebSocket(socket);
                }
            }

            bool success;
            try
            {
                success = Task.WaitAll(_tasks, 250);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Wait error: {e}");

                success = false;
            }

            if (success == false)
            {
                for (int i = 0; i < _tasks.Length; i++)
                {
                    if (_tasks[i].IsFaulted || _tasks[i].IsCanceled ||
                        _tasks[i].IsCompleted == false)
                    {
                        // this either timed out or errored, removing it.
                        RemoveWebSocket(item.WebSocketsList[i]);
                    }
                }
            }
        }

        private void RemoveWebSocket(WebSocket socket)
        {
            Console.WriteLine($"Removing socket: {socket.CloseStatusDescription}");

            WebSocketContext value;
            _listeners.TryRemove(socket, out value);
            if (!_listeners.IsEmpty)
                return;

            lock (this)
            {
                if (_listeners.IsEmpty)
                {
                    SetupLogMode(_oldLogMode, _path);
                }
            }
        }

        public void EnableConsoleLogging()
        {
            _additionalOutput = Console.OpenStandardOutput();
        }

        public void DisableConsoleLogging()
        {
            using (_additionalOutput)
            {
                _additionalOutput = null;
            }
        }

        private class LocalThreadWriterState
        {
            public int Generation;

            public readonly ForwardingStream ForwardingStream;

            public readonly SingleProducerSingleConsumerCircularQueue<WebSocketMessageEntry> Free =
                new SingleProducerSingleConsumerCircularQueue<WebSocketMessageEntry>(1024);

            public readonly SingleProducerSingleConsumerCircularQueue<WebSocketMessageEntry> Full =
                new SingleProducerSingleConsumerCircularQueue<WebSocketMessageEntry>(1024);

            public readonly StreamWriter Writer;

            public LocalThreadWriterState()
            {
                ForwardingStream = new ForwardingStream();
                Writer = new StreamWriter(ForwardingStream);
            }

#pragma warning disable 414
            public string OwnerThread;
            public int ThreadId;
#pragma warning restore 414
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
