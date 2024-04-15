using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Binary;
using Sparrow.Collections;
using Sparrow.Extensions;
using Sparrow.Platform;
using Sparrow.Threading;
using Sparrow.Utils;
using NativeMemory = Sparrow.Utils.NativeMemory;

namespace Sparrow.Logging
{
    public sealed class LoggingSource
    {
        [ThreadStatic]
        private static string _currentThreadId;

        public static bool UseUtcTime;
        public long MaxFileSizeInBytes = 1024 * 1024 * 128;
        private const string DateTimeWithMinutesFormat = "yyyy-MM-dd-HH-mm"; // current format
        private const string DateOnlyFormat = "yyyy-MM-dd"; // for backward compatibility
        internal const string LogExtension = ".log";
        internal const string AdditionalCompressExtension = ".gz";
        internal const string FullCompressExtension = LogExtension + AdditionalCompressExtension;

        internal static long LocalToUtcOffsetInTicks;

        static LoggingSource()
        {
            LocalToUtcOffsetInTicks = TimeZoneInfo.Local.GetUtcOffset(DateTime.Now).Ticks;
            ThreadLocalCleanup.ReleaseThreadLocalState += () => _currentThreadId = null;
        }

        private readonly ManualResetEventSlim _hasEntries = new ManualResetEventSlim(false);
        private readonly ManualResetEventSlim _readyToCompress = new ManualResetEventSlim(false);
        private readonly LightWeightThreadLocal<LocalThreadWriterState> _localState;

        private readonly LimitedConcurrentSet<LogMessageEntry>[] _freePooledMessageEntries;
        private readonly LimitedConcurrentSet<LogMessageEntry>[] _activePoolMessageEntries;

        private Thread _loggingThread;
        private Thread _compressLoggingThread;
        private int _generation;

        private bool _updateLocalTimeOffset;
        private string _path;
        private readonly string _name;
        private string _dateString;
        private readonly MultipleUseFlag _keepLogging = new MultipleUseFlag(true);
        private int _logNumber = -1;
        private DateTime _today;
        private bool _isInfoEnabled;
        private bool _isOperationsEnabled;

        public bool IsInfoEnabled => _isInfoEnabled; 
        public bool IsOperationsEnabled => _isOperationsEnabled;

        private Stream _additionalOutput;

        private Stream _pipeSink;
        private static readonly int TimeToWaitForLoggingToEndInMilliseconds = 5_000;

        public static readonly LoggingSource Instance = new LoggingSource(LogMode.None, Path.GetTempPath(), "Logging", TimeSpan.FromDays(3), long.MaxValue)
        {
            _updateLocalTimeOffset = true
        };
        public static readonly LoggingSource AuditLog = new LoggingSource(LogMode.None, Path.GetTempPath(), "Audit Log", TimeSpan.MaxValue, long.MaxValue);

        private static readonly byte[] _headerRow =
            Encodings.Utf8.GetBytes($"Time, Thread, Level, Source, Logger, Message, Exception{Environment.NewLine}");

        public class WebSocketContext
        {
            public LoggingFilter Filter { get; } = new LoggingFilter();
        }

        private readonly ConcurrentDictionary<WebSocket, WebSocketContext> _listeners =
            new ConcurrentDictionary<WebSocket, WebSocketContext>();

        public LogMode LogMode { get; private set; }
        public TimeSpan RetentionTime { get; private set; }
        public long RetentionSize { get; private set; }
        public bool Compressing => _compressLoggingThread != null;


        private (bool Info, bool Operation) CalculateIsLogEnabled(LogMode? logMode = null)
        {
            if (_listeners.IsEmpty == false || _pipeSink != null) 
                return (true, true);
            
            logMode ??= LogMode;
            var info = (logMode & LogMode.Information) == LogMode.Information;
            var operation = (logMode & LogMode.Operations) == LogMode.Operations;
            return (info, operation);
        }
        
        public async Task Register(WebSocket source, WebSocketContext context, CancellationToken token)
        {
            await source.SendAsync(new ArraySegment<byte>(_headerRow), WebSocketMessageType.Text, true, token).ConfigureAwait(false);

            lock (this)
            {
                if (_listeners.TryAdd(source, context) == false)
                    throw new InvalidOperationException("Socket was already added?");
                if (LogMode == LogMode.None)
                {
                    SetupLogMode(LogMode, _path, RetentionTime, RetentionSize, Compressing);
                }
                else
                {
                    (_isInfoEnabled, _isOperationsEnabled) = (true, true);
                }
            }

            AssertLogging();

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
                    arraySegment = new ArraySegment<byte>(new byte[Bits.PowerOf2(maxBytes)]);

                var numberOfBytes = Encodings.Utf8.GetBytes(commandResult, 0,
                    commandResult.Length,
                    arraySegment.Array,
                    0);

                await source.SendAsync(new ArraySegment<byte>(arraySegment.Array, 0, numberOfBytes),
                    WebSocketMessageType.Text, true,
                    token).ConfigureAwait(false);
            }
        }

        private void AssertLogging()
        {
            var thread = _loggingThread;
            if (thread == null)
                throw new InvalidOperationException("There is no logging thread.");

            if (_keepLogging == false)
                throw new InvalidOperationException("Logging is turned off.");
        }

        public LoggingSource(LogMode logMode, string path, string name, TimeSpan retentionTime, long retentionSize, bool compress = false)
        {
            _path = path;
            _name = name;
            _localState = new LightWeightThreadLocal<LocalThreadWriterState>(GenerateThreadWriterState);
            _freePooledMessageEntries = new LimitedConcurrentSet<LogMessageEntry>[Environment.ProcessorCount];
            _activePoolMessageEntries = new LimitedConcurrentSet<LogMessageEntry>[Environment.ProcessorCount];
            for (int i = 0; i < _freePooledMessageEntries.Length; i++)
            {
                _freePooledMessageEntries[i] = new LimitedConcurrentSet<LogMessageEntry>(1000);
            }
            for (int i = 0; i < _activePoolMessageEntries.Length; i++)
            {
                _activePoolMessageEntries[i] = new LimitedConcurrentSet<LogMessageEntry>(1000);
            }

            SetupLogMode(logMode, path, retentionTime, retentionSize, compress);
        }

        public void SetupLogMode(LogMode logMode, string path, TimeSpan? retentionTime, long? retentionSize, bool compress)
        {
            SetupLogMode(logMode, path, retentionTime ?? TimeSpan.MaxValue, retentionSize ?? long.MaxValue, compress);
        }

        public void SetupLogMode(LogMode logMode, string path, TimeSpan retentionTime, long retentionSize, bool compress)
        {
            lock (this)
            {
                var copyLoggingThread = _loggingThread;
                if (copyLoggingThread?.ManagedThreadId == Thread.CurrentThread.ManagedThreadId)
                {
                    Task.Run(() => SetupLogMode(logMode, path, retentionTime, retentionSize, compress));
                    return;
                }
                (bool info, bool operation) old = (_isInfoEnabled, _isOperationsEnabled);
                (_isInfoEnabled, _isOperationsEnabled) = CalculateIsLogEnabled(logMode);
                if (_isInfoEnabled == old.info && _isOperationsEnabled == old.operation && LogMode == logMode && path == _path && retentionTime == RetentionTime && compress == Compressing)
                    return;
                LogMode = logMode;
                _path = path;
                RetentionTime = retentionTime;
                RetentionSize = retentionSize;

                Directory.CreateDirectory(_path);
                if (copyLoggingThread == null)
                {
                    StartNewLoggingThreads(compress);
                }
                else
                {
                    _keepLogging.Lower();
                    _hasEntries.Set();
                    _readyToCompress.Set();

                    copyLoggingThread.Join();
                    _compressLoggingThread?.Join();

                    StartNewLoggingThreads(compress);
                }
            }
        }

        private void StartNewLoggingThreads(bool compress)
        {
            if (IsInfoEnabled == false &&
                IsOperationsEnabled == false)
                return;

            _keepLogging.Raise();
            _loggingThread = new Thread(BackgroundLogger) { IsBackground = true, Name = _name + " Thread" };
            _loggingThread.Start();
            if (compress)
            {
                _compressLoggingThread = new Thread(BackgroundLoggerCompress) { IsBackground = true, Name = _name + " Log Compression Thread" };
                _compressLoggingThread.Start();
            }
            else
            {
                _compressLoggingThread = null;
            }
        }

        public void EndLogging()
        {
            lock (this)
            {
                _keepLogging.Lower();

                _hasEntries.Set();
                _readyToCompress.Set();

                _loggingThread?.Join(TimeToWaitForLoggingToEndInMilliseconds);
                _compressLoggingThread?.Join(TimeToWaitForLoggingToEndInMilliseconds);
            }
        }

        private bool TryGetNewStreamAndApplyRetentionPolicies(long maxFileSize, out FileStream fileStream)
        {
            string[] allLogFiles;
            try
            {
                var logFiles = Directory.GetFiles(_path, $"*{LogExtension}");
                var logGzFiles = Directory.GetFiles(_path, $"*{FullCompressExtension}");
                allLogFiles = logFiles.Concat(logGzFiles).ToArray();
            }
            catch (Exception)
            {
                // Something went wrong we will try again later
                fileStream = null;
                return false;
            }

            _today = DateTime.Today;
            (_logNumber, _dateString) = GetLastLogNumberAndDateStringForToday(allLogFiles);

            UpdateLocalDateTimeOffset();

            string filePath;
            while (true)
            {
                var fileName = $"{_dateString}.{_logNumber:000}{LogExtension}";
                filePath = Path.Combine(_path, fileName);
                if (File.Exists(filePath))
                {
                    var currentFileSize = new FileInfo(filePath).Length;
                    if (currentFileSize < maxFileSize)
                        break; // we didn't reach the size limit yet
                }
                else if (File.Exists($"{filePath}{AdditionalCompressExtension}") == false)
                {
                    break; // check if there is compressed file with the same name
                }
                _logNumber++;
            }

            // If compression for log files is enabled, we apply retention rules to logs inside the compressLoggingThread
            if (Compressing == false)
                ApplyRetentionRulesToLogs(allLogFiles);

            fileStream = SafeFileStream.Create(filePath, FileMode.Append, FileAccess.Write, FileShare.Read, 32 * 1024, false);
            fileStream.Write(_headerRow, 0, _headerRow.Length);
            return true;
        }

        private (string FullName, long Size) GetFileInfoSafe(string fileName)
        {
            var fileInfo = new FileInfo(fileName);
            long fileSize = 0;
            try
            {
                fileSize = fileInfo.Length;
            }
            catch
            {
                // Many things can happen
            }

            return (fileInfo.FullName, fileSize);
        }

        internal static bool TryGetLastWriteTimeUtc(string filePath, out DateTime dateTimeUtc) =>
                TryGetFileTimeInternal(filePath, File.GetLastWriteTimeUtc, out dateTimeUtc);

        internal static bool TryGetLastWriteTimeLocal(string filePath, out DateTime dateTimeLocal) =>
            TryGetFileTimeInternal(filePath, File.GetLastWriteTime, out dateTimeLocal);

        internal static bool TryGetCreationTimeUtc(string filePath, out DateTime dateTimeUtc) =>
            TryGetFileTimeInternal(filePath, fp => GetLogFileCreationTime(fp, DateTimeKind.Utc), out dateTimeUtc);

        internal static bool TryGetCreationTimeLocal(string filePath, out DateTime dateTimeLocal) =>
            TryGetFileTimeInternal(filePath, fp => GetLogFileCreationTime(fp, DateTimeKind.Local), out dateTimeLocal);

        private static bool TryGetFileTimeInternal(string filePath, Func<string, DateTime> fileTimeGetter, out DateTime dateTime)
        {
            dateTime = default;
            try
            {
                if (filePath.Contains(Path.DirectorySeparatorChar) && File.Exists(filePath) == false)
                    return false;

                dateTime = fileTimeGetter(filePath);
                return true;
            }
            catch
            {
                // ignored
            }

            return false;
        }

        internal static DateTime GetLogFileCreationTime(string filePathOrName, DateTimeKind timeKind)
        {
            var fileName = filePathOrName.Contains(Path.DirectorySeparatorChar) ? Path.GetFileName(filePathOrName) : filePathOrName;
            var timestamp = fileName.Substring(0, fileName.IndexOf('.'));

            if (DateTime.TryParseExact(timestamp, DateTimeWithMinutesFormat, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var dateTime) == false &&
                DateTime.TryParseExact(timestamp, DateOnlyFormat, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out dateTime) == false) // backward compatibility
                throw new InvalidOperationException($"Could not parse the log file name '{fileName}'");

            return timeKind switch
            {
                DateTimeKind.Utc => dateTime.ToUniversalTime(),
                DateTimeKind.Local => dateTime,
                _ => throw new ArgumentOutOfRangeException(nameof(timeKind), timeKind, null)
            };
        }

        internal static bool TryGetLogFileNumber(string filePathOrName, out int n)
        {
            n = -1;
            var fileName = filePathOrName.Contains(Path.DirectorySeparatorChar) ? Path.GetFileName(filePathOrName) : filePathOrName;

            var firstDotIndex = fileName.IndexOf('.');
            if (firstDotIndex == -1)
                return false;

            var secondDotIndex = fileName.IndexOf('.', firstDotIndex + 1);
            if (secondDotIndex == -1)
                return false;

            var betweenDots = fileName.Substring(firstDotIndex + 1, secondDotIndex - firstDotIndex - 1);
            return int.TryParse(betweenDots, out n);
        }

        internal static string DateToLogFormat(DateTime dateTime)
        {
            return dateTime.ToString(DateTimeWithMinutesFormat, CultureInfo.InvariantCulture);
        }

        private void UpdateLocalDateTimeOffset()
        {
            if (_updateLocalTimeOffset == false || UseUtcTime)
                return;

            var offset = TimeZoneInfo.Local.GetUtcOffset(DateTime.Now).Ticks;
            if (offset != LocalToUtcOffsetInTicks)
                Interlocked.Exchange(ref LocalToUtcOffsetInTicks, offset);
        }

        private static (int LogNumber, string DateString) GetLastLogNumberAndDateStringForToday(IEnumerable<string> allLogFiles)
        {
            var now = DateTime.Now;
            var todayPrefix = now.ToString(DateOnlyFormat);

            var todayLogs = allLogFiles
                .Select(Path.GetFileName)
                .Where(fileName => fileName.StartsWith(todayPrefix))
                .OrderBy(fileName => fileName)
                .ToArray();

            for (int i = todayLogs.Length - 1; i >= 0; i--)
            {
                if (TryGetCreationTimeLocal(todayLogs[i], out DateTime logDateTime) == false ||
                    TryGetLogFileNumber(todayLogs[i], out var logNumber) == false)
                    continue;

                return (logNumber, DateToLogFormat(logDateTime));
            }

            return (0, DateToLogFormat(now));
        }

        private void ApplyRetentionRulesToLogs(string[] logFiles)
        {
            if (logFiles == null || logFiles.Length == 0)
                return;

            if (RetentionTime != TimeSpan.MaxValue)
                logFiles = CleanupOldLogFiles(logFiles);

            LimitTotalLogSize(logFiles);
        }

        private string[] CleanupOldLogFiles(string[] logFiles)
        {
            for (int i = 0; i < logFiles.Length; i++)
            {
                var logFile = logFiles[i];
                if (TryGetLastWriteTimeLocal(logFile, out var logDateTime) == false ||
                    DateTime.Now - logDateTime <= RetentionTime)
                    continue;

                try
                {
                    File.Delete(logFile);
                    logFiles[i] = null;
                }
                catch (Exception)
                {
                    // we don't actually care if we can't handle this scenario, we'll just try again later
                    // maybe something is currently reading the file?
                }
            }

            return logFiles.Where(file => file != null).ToArray();
        }

        private void LimitTotalLogSize(string[] logFiles)
        {
            if (logFiles == null)
                return;

            var logFilesInfo = logFiles.Select(GetFileInfoSafe).ToArray();
            var totalLogSize = logFilesInfo.Sum(i => i.Size);

            long retentionSizeMinusCurrentFile = RetentionSize - MaxFileSizeInBytes;
            foreach (var log in logFilesInfo)
            {
                if (totalLogSize > retentionSizeMinusCurrentFile)
                {
                    try
                    {
                        File.Delete(log.FullName);
                    }
                    catch
                    {
                        // Something went wrong we will try again later
                        continue;
                    }
                    totalLogSize -= log.Size;
                }
                else
                {
                    return;
                }
            }
        }

        private static void CleanupAlreadyCompressedLogFiles(string[] sortedLogFiles, string[] sortedLogGzFiles)
        {
            if (!sortedLogGzFiles.Any())
                return;

            foreach (var logFile in sortedLogFiles)
            {
                try
                {
                    if (Array.BinarySearch(sortedLogGzFiles, logFile, Comparer) > 0)
                    {
                        File.Delete(logFile);
                    }
                }
                catch (Exception)
                {
                    // we don't actually care if we can't handle this scenario, we'll just try again later
                    // maybe something is currently reading the file?
                }
            }
        }

        private static readonly IComparer<string> Comparer = new LogComparer();
        private class LogComparer : IComparer<string>
        {
            public int Compare(string x, string y)
            {
                string xFileName = Path.GetFileName(x);
                var xJustFileName = xFileName.Substring(0, xFileName.LastIndexOf(LogExtension, StringComparison.Ordinal));
                var yFileName = Path.GetFileName(y);
                var yJustFileName = yFileName.Substring(0, yFileName.LastIndexOf(LogExtension, StringComparison.Ordinal));
                return string.CompareOrdinal(xJustFileName, yJustFileName);
            }
        }

        private LocalThreadWriterState GenerateThreadWriterState()
        {
            var currentThread = Thread.CurrentThread;
            return new LocalThreadWriterState
            {
                OwnerThread = currentThread.Name,
                ThreadId = currentThread.ManagedThreadId,
                Generation = _generation
            };
        }
        public void Log(ref LogEntry entry, TaskCompletionSource<object> tcs = null)
        {
            var state = _localState.Value;
            if (state.Generation != _generation)
            {
                state = _localState.Value = GenerateThreadWriterState();
            }

            int currentProcessNumber = CurrentProcessorIdHelper.GetCurrentProcessorId() % _freePooledMessageEntries.Length;
            var pool = _freePooledMessageEntries[currentProcessNumber];

            if (pool.TryDequeue(out var item))
            {
                item.Data.SetLength(0);
                item.WebSocketsList.Clear();
                item.Task = tcs;
                state.ForwardingStream.Destination = item.Data;
            }
            else
            {
                item = new LogMessageEntry { Task = tcs };
                state.ForwardingStream.Destination = new MemoryStream();
            }

            if (_listeners.IsEmpty == false)
            {
                foreach (var kvp in _listeners)
                {
                    if (kvp.Value.Filter.Forward(ref entry))
                    {
                        item.WebSocketsList.Add(kvp.Key);
                    }
                }
            }

            WriteEntryToWriter(state.Writer, ref entry);
            item.Data = state.ForwardingStream.Destination;
            Debug.Assert(item.Data != null);
            item.Type = entry.Type;
            
            _activePoolMessageEntries[currentProcessNumber].Enqueue(item, 128);

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
                var threadStatesToRemove = new FastStack<WeakReference<LocalThreadWriterState>>();
                while (_keepLogging)
                {
                    try
                    {
                        var maxFileSize = MaxFileSizeInBytes;
                        if (TryGetNewStreamAndApplyRetentionPolicies(maxFileSize, out var currentFile) == false)
                        {
                            if (_keepLogging == false)
                                return;
                            _hasEntries.Wait(1000);
                            continue;
                        }

                        using (currentFile)
                        {
                            _readyToCompress.Set();

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
                                    {
                                        // about to go to sleep, so can check if need to update offset or create new file for today logs
                                        UpdateLocalDateTimeOffset();

                                        if (DateTime.Today != _today)
                                        {
                                            // let's create new file so its name will have today date
                                            break;
                                        }
                                    }

                                    _hasEntries.Wait();
                                    if (_keepLogging == false)
                                        return;

                                    _hasEntries.Reset();
                                }

                                foundEntry = false;
                                for (var index = 0; index < _activePoolMessageEntries.Length; index++)
                                {
                                    var messages = _activePoolMessageEntries[index];
                                    for (var limit = 0; limit < 16; limit++)
                                    {
                                        if (messages.TryDequeue(out LogMessageEntry item) == false)
                                            break;

                                        foundEntry = true;

                                        sizeWritten += ActualWriteToLogTargets(item, currentFile);
                                        Debug.Assert(item.Data != null);
                                        _freePooledMessageEntries[index].Enqueue(item, 128);
                                    }
                                }
                            }
                        }
                    }
                    catch (OutOfMemoryException)
                    {
                        Console.Error.WriteLine("Out of memory exception while trying to log, will avoid logging for the next 5 seconds");

                        DisableLogsFor(TimeSpan.FromSeconds(5));
                    }
                    catch (Exception e)
                    {
                        var msg = e is IOException i && IsOutOfDiskSpaceException(i)
                            ? "Couldn't create a new log file because of out of disk space! " +
                              "Disabling the logs for 30 seconds"
                            : "FATAL ERROR trying to log!";

                        Console.Error.WriteLine($"{msg}{Environment.NewLine}{e}");

                        DisableLogsFor(TimeSpan.FromSeconds(30));
                    }
                }
            }
            finally
            {
                _readyToCompress.Set();
                _compressLoggingThread?.Join();
            }
        }

        private void DisableLogsFor(TimeSpan timeout)
        {
            try
            {
                _isInfoEnabled = false;
                _isOperationsEnabled = false;

                foreach (var queue in _activePoolMessageEntries)
                {
                    queue.Clear();
                }

                Thread.Sleep(timeout);
            }
            finally
            {
                (_isInfoEnabled, _isOperationsEnabled) = CalculateIsLogEnabled();
            }
        }

        private static bool IsOutOfDiskSpaceException(IOException ioe)
        {
            const int posixOutOfDiskSpaceError = 28; // Errno.ENOSPC
            const int windowsOutOfDiskSpaceError = 0x70; // Win32NativeFileErrors.ERROR_DISK_FULL

            var expectedDiskFullError = PlatformDetails.RunningOnPosix ? posixOutOfDiskSpaceError : windowsOutOfDiskSpaceError;
            var errorCode = PlatformDetails.RunningOnPosix ? ioe.HResult : ioe.HResult & 0xFFFF;
            return errorCode == expectedDiskFullError;
        }

        private void BackgroundLoggerCompress()
        {
            var logger = GetLogger($"{nameof(LoggingSource)}", $"{nameof(BackgroundLoggerCompress)}");
            var keepCompress = true;
            while (true)
            {
                try
                {
                    if (keepCompress == false)
                        return;

                    if (_keepLogging == false)
                    {
                        //To do last round of compression after stop logging
                        keepCompress = false;
                    }
                    else
                    {
                        _readyToCompress.Wait();
                        _readyToCompress.Reset();
                    }

                    string[] logFiles;
                    string[] logGzFiles;
                    try
                    {
                        logFiles = Directory.GetFiles(_path, $"*{LogExtension}");
                        logGzFiles = Directory.GetFiles(_path, $"*{FullCompressExtension}");
                    }
                    catch (Exception)
                    {
                        // Something went wrong we will try again later
                        continue;
                    }

                    if (logFiles.Length <= 1)
                        // There is only one log file in the middle of writing
                        continue;

                    Array.Sort(logFiles);
                    Array.Sort(logGzFiles);

                    for (var i = 0; i < logFiles.Length - 1; i++)
                    {
                        var logFile = logFiles[i];
                        if (Array.BinarySearch(logGzFiles, logFile) > 0)
                            continue; // Already compressed

                        try
                        {
                            var newZippedFilePath = Path.Combine(_path, Path.GetFileNameWithoutExtension(logFile) + FullCompressExtension);
                            using (var logStream = SafeFileStream.Create(logFile, FileMode.Open, FileAccess.Read))
                            {
                                // If there is compressed file with the same name (probably due to a failure) it will be overwritten
                                using (var newFileStream = SafeFileStream.Create(newZippedFilePath, FileMode.Create, FileAccess.Write))
                                using (var compressionStream = new GZipStream(newFileStream, CompressionMode.Compress))
                                {
                                    logStream.CopyTo(compressionStream);
                                }
                            }
                            File.SetLastWriteTime(newZippedFilePath, File.GetLastWriteTime(logFile));
                        }
                        catch (Exception)
                        {
                            //Something went wrong we will try later again
                            continue;
                        }

                        try
                        {
                            File.Delete(logFile);
                        }
                        catch (Exception)
                        {
                            // we don't actually care if we can't handle this scenario, we'll just try again later
                            // maybe something is currently reading the file?
                        }
                    }

                    CleanupAlreadyCompressedLogFiles(logFiles, logGzFiles);
                    ApplyRetentionRulesToLogs(logGzFiles);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception e)
                {
                    if (logger.IsOperationsEnabled)
                        logger.Operations("Something went wrong while compressing log files", e);
                }
            }
        }

        public void AttachPipeSink(Stream stream)
        {
            _pipeSink = stream;
            if (LogMode == LogMode.None)
            {
                SetupLogMode(LogMode, _path, RetentionTime, RetentionSize, Compressing);
            }
            else
            {
                (_isInfoEnabled, _isOperationsEnabled) = (true, true);
            }
        }

        public void DetachPipeSink()
        {
            _pipeSink = null;
            if (LogMode == LogMode.None)
            {
                SetupLogMode(LogMode, _path, RetentionTime, RetentionSize, Compressing);
            }
            else
            {
                (_isInfoEnabled, _isOperationsEnabled) = CalculateIsLogEnabled();
            }
        }

        private int ActualWriteToLogTargets(LogMessageEntry item, Stream file)
        {
            item.Data.TryGetBuffer(out var bytes);
            Debug.Assert(bytes.Array != null);

            if (item.Type == LogMode.Operations && LogMode != LogMode.None || (LogMode & LogMode.Information) == LogMode.Information)
            {
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
            }
            else
            {
                item.Task?.TrySetResult(null);
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

        private void SendToWebSockets(LogMessageEntry item, ArraySegment<byte> bytes)
        {
            if (_tasks.Length != item.WebSocketsList.Count)
                Array.Resize(ref _tasks, item.WebSocketsList.Count);

            for (int i = 0; i < item.WebSocketsList.Count; i++)
            {
                var socket = item.WebSocketsList[i];
                try
                {
                    _tasks[i] = socket.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None);
                }
                catch (Exception e)
                {
                    RemoveWebSocket(socket, e.ToString());
                }
            }

            try
            {
                if (Task.WaitAll(_tasks, 250)) 
                    return;
            }
            catch
            {
                // ignored
            }

            for (int i = 0; i < _tasks.Length; i++)
            {
                var task = _tasks[i];
                string error = null;
                if (task.IsFaulted)
                {
                    error = task.Exception?.ToString() ?? "Faulted";
                }
                else if (task.IsCanceled)
                {
                    error = "Canceled";
                }
                else if (task.IsCompleted == false)
                {
                    error = "Timeout - 250 milliseconds";
                }
                if(error != null)
                    RemoveWebSocket(item.WebSocketsList[i], error);
            }
        }

        private void RemoveWebSocket(WebSocket socket, string cause)
        {
            try
            {
                //To release the socket.ReceiveAsync call in Register function we must to close the socket 
                socket.CloseAsync(WebSocketCloseStatus.EndpointUnavailable, cause, CancellationToken.None)
                    .ContinueWith(t => GC.KeepAlive(t.Exception), TaskContinuationOptions.ExecuteSynchronously | TaskContinuationOptions.OnlyOnFaulted);
            }
            catch
            {
                // ignored
            }

            _listeners.TryRemove(socket, out _);
            if (LogMode == LogMode.None)
            {
                SetupLogMode(LogMode, _path, RetentionTime, RetentionSize, Compressing);
            }
            else
            {
                (_isInfoEnabled, _isOperationsEnabled) = CalculateIsLogEnabled();
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
