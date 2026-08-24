using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server;
using Sparrow.Server.Logging;
using Sparrow.Server.Meters;
using Sparrow.Server.Platform;
using Sparrow.Server.Utils;
using Sparrow.Utils;
using Voron.Exceptions;
using Voron.Impl;
using Voron.Impl.FileHeaders;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Impl.Scratch;
using Voron.Logging;
using Voron.Platform.Posix;
using Voron.Util;
using Voron.Util.Settings;
using Constants = Voron.Global.Constants;

namespace Voron
{
    public abstract class StorageEnvironmentOptions : IDisposable
    {
        private ExceptionDispatchInfo _catastrophicFailure;
        private string _catastrophicFailureStack;

        [ThreadStatic]
        private static bool _skipCatastrophicFailureAssertion;
        private readonly CatastrophicFailureNotification _catastrophicFailureNotification;

        public abstract (Pager Pager, Pager.State State) InitializeDataPager();
        
        public readonly LoggingResource LoggingResource;

        public readonly LoggingComponent LoggingComponent;

        public VoronPathSetting TempPath { get; }

        public VoronPathSetting JournalPath { get; private set; }

        public IoMetrics IoMetrics { get; set; }

        public bool GenerateNewDatabaseId { get; set; }
        
        public LazyWithExceptionRetry<DriveInfoByPath> DriveInfoByPath { get; private set; }

        public event EventHandler<RecoveryErrorEventArgs> OnRecoveryError;
        public event EventHandler<NonDurabilitySupportEventArgs> OnNonDurableFileSystemError;
        public event EventHandler<DataIntegrityErrorEventArgs> OnIntegrityErrorOfAlreadySyncedData;
        public event EventHandler<RecoverableFailureEventArgs> OnRecoverableFailure;

        public abstract override string ToString();

        private bool _forceUsing32BitsPager;
        public bool ForceUsing32BitsPager
        {
            get => _forceUsing32BitsPager;
            set
            {
                _forceUsing32BitsPager = value;
                MaxLogFileSize = (value ? 32 : 256) * Constants.Size.Megabyte;
                MaxScratchBufferSize = (value ? 32 : 256) * Constants.Size.Megabyte;
                MaxNumberOfPagesInJournalBeforeFlush = (value ? 4 : 32) * Constants.Size.Megabyte / Constants.Storage.PageSize;
            }
        }

        public bool EnablePrefetching = true;

        internal DisposableAction DisableOnRecoveryErrorHandler()
        {
            var handler = OnRecoveryError;
            OnRecoveryError = (_, __) => { };

            return new DisposableAction(() => OnRecoveryError = handler);
        }

        public void InvokeRecoveryError(object sender, string message, Exception e)
        {
            var handler = OnRecoveryError;
            if (handler == null)
            {
                throw new InvalidDataException(message + Environment.NewLine +
                                               $"An exception has been thrown because there isn't a listener to the {nameof(OnRecoveryError)} event on the storage options.", e);
            }

            handler(sender, new RecoveryErrorEventArgs(message, e));
        }

        internal DisposableAction DisableOnIntegrityErrorOfAlreadySyncedDataHandler()
        {
            var handler = OnIntegrityErrorOfAlreadySyncedData;
            OnIntegrityErrorOfAlreadySyncedData = (_, __) => { };

            return new DisposableAction(() => OnIntegrityErrorOfAlreadySyncedData = handler);
        }

        public void InvokeIntegrityErrorOfAlreadySyncedData(object sender, string message, Exception e)
        {
            var handler = OnIntegrityErrorOfAlreadySyncedData;
            if (handler == null)
            {
                throw new InvalidDataException(message + Environment.NewLine +
                                               $"An exception has been thrown because there isn't a listener to the {nameof(OnIntegrityErrorOfAlreadySyncedData)} event on the storage options.", e);
            }

            handler(this, new DataIntegrityErrorEventArgs(message, e));
        }

        public void InvokeNonDurableFileSystemError(object sender, string message, Exception e, string details)
        {
            var handler = OnNonDurableFileSystemError;
            if (handler == null)
            {
                throw new InvalidDataException(message + Environment.NewLine +
                                               "An exception has been thrown because there isn't a listener to the OnNonDurableFileSystemError event on the storage options.",
                    e);
            }

            handler(this, new NonDurabilitySupportEventArgs(message, e, details));
        }

        public long? InitialFileSize { get; set; }

        public long MaxLogFileSize
        {
            get { return _maxLogFileSize; }
            set
            {
                if (value < _initialLogFileSize)
                    InitialLogFileSize = value;
                _maxLogFileSize = value;
            }
        }

        public long InitialLogFileSize
        {
            get { return _initialLogFileSize; }
            set
            {
                if (value > MaxLogFileSize)
                    MaxLogFileSize = value;
                if (value <= 0)
                    ThrowInitialLogFileSizeOutOfRange();
                _initialLogFileSize = value;
            }
        }

        [DoesNotReturn]
        private static void ThrowInitialLogFileSizeOutOfRange()
        {
            throw new ArgumentOutOfRangeException("InitialLogFileSize", "The initial log for the Voron must be above zero");
        }

        public StorageEncryptionOptions Encryption { get; } = new StorageEncryptionOptions();

        public int PageSize => Constants.Storage.PageSize;

        // if set to a non zero value, will check that the expected schema is there
        public int SchemaVersion { get; set; }

        public UpgraderDelegate SchemaUpgrader { get; set; }
        
        public Action<Transaction> OnVersionReadingTransaction { get; set; }

        public Action<StorageEnvironment> BeforeSchemaUpgrade { get; set; }

        public Action<StorageEnvironment> AfterDatabaseCreation { get; set; }

        public ScratchSpaceUsageMonitor ScratchSpaceUsage { get; }

        public TimeSpan LongRunningFlushingWarning = TimeSpan.FromMinutes(5);

        public long MaxScratchBufferSize
        {
            get => _maxScratchBufferSize;
            set
            {
                if (value < 0)
                    throw new InvalidOperationException($"Cannot set {nameof(MaxScratchBufferSize)} to negative value: {value}");

                const long maxSupportedScratchBufferSize = (long)uint.MaxValue * Constants.Storage.PageSize;
                if (value > maxSupportedScratchBufferSize)
                    throw new InvalidOperationException(
                        $"Cannot set {nameof(MaxScratchBufferSize)} to {value:#,#} bytes: a position inside a scratch buffer must fit in 32 bits (as pages), so the maximum supported value is {maxSupportedScratchBufferSize:#,#} bytes");

                if (_forceUsing32BitsPager && _maxScratchBufferSize > 0)
                {
                    _maxScratchBufferSize = Math.Min(value, _maxScratchBufferSize);
                    return;
                }

                _maxScratchBufferSize = value;
            }
        }

        public bool OwnsPagers { get; set; }

        public bool ManualFlushing { get; set; }

        public bool IncrementalBackupEnabled { get; set; }

        public long MaxNumberOfPagesInJournalBeforeFlush { get; set; }

        public int IdleFlushTimeout { get; set; }

        public long? MaxStorageSize { get; set; }

        public abstract VoronPathSetting BasePath { get; }

        /// <summary>
        /// This mode is used in the Voron recovery tool and is not intended to be set otherwise.
        /// </summary>
        internal bool CopyOnWriteMode { get; set; }

        public abstract void LinkFiles(long journalNumber, string filePath, out string finalFilePath);

        public abstract bool IsLinked(long journalNumber, string filePath, out string finalFilePath);
        
        public abstract JournalWriter CreateJournalWriter(long journalNumber, long journalSize);

        public abstract JournalWriter CreateNewJournalWriter(long journalNumber, long minRequiredSize, long preferredSize, Pal.journal_entry journalHeaderRecord, WriteAheadJournal journal);

        public virtual void PrepareRecyclableJournalInBackground(long size, WriteAheadJournal journal)
        {
        }

        public bool EnableJournalPoolPrewarming { get; set; } = true;

        public abstract JournalWriter CreateReadOnlyJournalWriter(long journalNumber, long journalSize);

        public abstract JournalWriter CreateJournalWriterForBranchEnvironment(long journalNumber, string fileName, JournalFile journalFile);

        public abstract void DeleteJournalsBelow(long journalNumber);

        public abstract VoronPathSetting GetJournalPath(long journalNumber);

        public virtual bool IsJournalHardLinked(long journalNumber) => false;

        protected bool Disposed;
        private long _initialLogFileSize;
        private long _maxLogFileSize;

        public Func<string, bool> ShouldUseKeyPrefix { get; set; }

        public Action<LogLevel, string> AddToInitLog;

        public event Action<StorageEnvironmentOptions> OnDirectoryInitialize;

        private StorageEnvironmentOptions(VoronPathSetting tempPath, IoChangesNotifications ioChangesNotifications, CatastrophicFailureNotification catastrophicFailureNotification, LoggingResource loggingResource, LoggingComponent loggingComponent)
        {
            LoggingResource = loggingResource;
            LoggingComponent = loggingComponent;

            DisposeWaitTime = TimeSpan.FromSeconds(15);

            TempPath = tempPath;

            ShouldUseKeyPrefix = name => false;

            var shouldForceEnvVar = Environment.GetEnvironmentVariable("VORON_INTERNAL_ForceUsing32BitsPager");

            if (bool.TryParse(shouldForceEnvVar, out bool result))
                ForceUsing32BitsPager = result;

            bool shouldConfigPagersRunInLimitedMemoryEnvironment = PlatformDetails.Is32Bits || ForceUsing32BitsPager;
            MaxLogFileSize = ((shouldConfigPagersRunInLimitedMemoryEnvironment ? 4 : 256) * Constants.Size.Megabyte);            

            InitialLogFileSize = 64 * Constants.Size.Kilobyte;

            MaxScratchBufferSize = ((shouldConfigPagersRunInLimitedMemoryEnvironment ? 32 : 256) * Constants.Size.Megabyte);

            MaxNumberOfPagesInJournalBeforeFlush =
                ((shouldConfigPagersRunInLimitedMemoryEnvironment ? 4 : 32) * Constants.Size.Megabyte) / Constants.Storage.PageSize;

            IdleFlushTimeout = 5000; // 5 seconds

            OwnsPagers = true;

            IncrementalBackupEnabled = false;

            IoMetrics = ioChangesNotifications?.DisableIoMetrics == true ? 
                new IoMetrics(0, 0) : // disabled
                new IoMetrics(256, 256, ioChangesNotifications);

            _log = RavenLogManager.Instance.GetLoggerForVoron<StorageEnvironmentOptions>(this, tempPath.FullPath);

            _catastrophicFailureNotification = catastrophicFailureNotification ?? new CatastrophicFailureNotification((id, path, e, stacktrace) =>
            {
                if (_log.IsFatalEnabled)
                    _log.Fatal($"Catastrophic failure in {this}, StackTrace:'{stacktrace}'", e);
            });

            PrefetchSegmentSize = 4 * Constants.Size.Megabyte;
            PrefetchResetThreshold = shouldConfigPagersRunInLimitedMemoryEnvironment?256*(long)Constants.Size.Megabyte: 8 * (long)Constants.Size.Gigabyte;
            SyncJournalsCountThreshold = 2;

            ScratchSpaceUsage = new ScratchSpaceUsageMonitor();
        }

        public void SetCatastrophicFailure(ExceptionDispatchInfo exception)
        {
            _catastrophicFailureStack = Environment.StackTrace;
            _catastrophicFailure = exception;
            _catastrophicFailureNotification.RaiseNotificationOnce(_environmentId, ToString(), exception.SourceException, _catastrophicFailureStack);
        }

        public void InvokeRecoverableFailure(string failureMessage, Exception e)
        {
            var handler = OnRecoverableFailure;

            if (handler != null)
            {
                handler.Invoke(this, new RecoverableFailureEventArgs(failureMessage, _environmentId, ToString(), e));
            }
            else
            {
                if (_log.IsDebugEnabled)
                    _log.Debug($"Recoverable failure in {this}. Error: {failureMessage}.", e);
            }
        }

        public bool IsCatastrophicFailureSet => _catastrophicFailure != null;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void AssertNoCatastrophicFailure()
        {
            if (_catastrophicFailure == null)
                return;

            if (_skipCatastrophicFailureAssertion)
                return;

            AssertNoCatastrophicFailureUnlikely();

            void AssertNoCatastrophicFailureUnlikely()
            {
                if (_log.IsFatalEnabled)
                    _log.Fatal($"CatastrophicFailure state, about to throw. Originally was set in the following stack trace : {_catastrophicFailureStack}");

                _catastrophicFailure.Throw(); // force re-throw of error
            }
        }

        public IDisposable SkipCatastrophicFailureAssertion()
        {
            _skipCatastrophicFailureAssertion = true;

            return new DisposableAction(() => { _skipCatastrophicFailureAssertion = false; });
        }

        public static StorageEnvironmentOptions CreateMemoryOnly(string name, string tempPath, IoChangesNotifications ioChangesNotifications, CatastrophicFailureNotification catastrophicFailureNotification, LoggingResource loggingResource, LoggingComponent loggingComponent)
        {
            var tempPathSetting = new VoronPathSetting(tempPath ?? GetTempPath());
            return new PureMemoryStorageEnvironmentOptions(name, tempPathSetting, ioChangesNotifications, catastrophicFailureNotification, loggingResource, loggingComponent);
        }

        public static StorageEnvironmentOptions CreateMemoryOnlyForTests([CallerMemberName] string caller = null, LoggingResource loggingResource = null, LoggingComponent loggingComponent = null)
        {
            return CreateMemoryOnly(caller, null, null, null, loggingResource, loggingComponent);
        }

        public static StorageEnvironmentOptions ForPath(string path, string tempPath, string journalPath, IoChangesNotifications ioChangesNotifications, CatastrophicFailureNotification catastrophicFailureNotification, LoggingResource loggingResource,
            LoggingComponent loggingComponent)
        {
            var pathSetting = new VoronPathSetting(path);
            var tempPathSetting = new VoronPathSetting(tempPath ?? GetTempPath(path));
            var journalPathSetting = journalPath != null ? new VoronPathSetting(journalPath) : pathSetting.Combine("Journals");

            return new DirectoryStorageEnvironmentOptions(pathSetting, tempPathSetting, journalPathSetting, ioChangesNotifications, catastrophicFailureNotification, loggingResource, loggingComponent);
        }

        public static StorageEnvironmentOptions ForPathForTests(string path, LoggingResource loggingResource = null, LoggingComponent loggingComponent = null)
        {
            return ForPath(path, null, null, null, null, loggingResource, loggingComponent);
        }

        private static string GetTempPath(string basePath = null)
        {
            bool useSystemTemp = false;
            // We need to use a Temp directory for storage. There's two ways to do this: either the user provides a full path
            // to use as base (because they expect all temporary files to be stored under it too), or we use the current
            // running directory.
            string tempPath = Path.Combine(basePath ?? Directory.GetCurrentDirectory(), "Temp");
            try
            {
                Directory.CreateDirectory(tempPath);
            }
            catch (UnauthorizedAccessException)
            {
                useSystemTemp = true;
            }

            if (!useSystemTemp)
            {
                // Effective permissions are hard to compute, so we try to create a file and write to it as a check.
                try
                {
                    var tempFilePath = Path.Combine(tempPath, Guid.NewGuid().ToString());
                    File.Create(tempFilePath, 1024).Dispose();
                    File.Delete(tempFilePath);
                }
                catch (Exception)
                {
                    useSystemTemp = true;
                }

            }

            if (useSystemTemp)
                tempPath = Path.GetTempPath();

            return tempPath;
        }

        public sealed class DirectoryStorageEnvironmentOptions : StorageEnvironmentOptions
        {
            public const string TempFileExtension = ".tmp";
            public const string BuffersFileExtension = ".buffers";

            private readonly VoronPathSetting _basePath;

            private readonly ConcurrentDictionary<string, LazyWithExceptionRetry<JournalWriter>> _journals = new(StringComparer.OrdinalIgnoreCase);

            private readonly SortedList<long, string> _journalsForReuse = new();
            private long _reuseCounter;
            private long _lastJournalCreatedTicks;
            private long _journalFillIntervalTicks;

            public DirectoryStorageEnvironmentOptions(VoronPathSetting basePath, VoronPathSetting tempPath, VoronPathSetting journalPath,
                IoChangesNotifications ioChangesNotifications, CatastrophicFailureNotification catastrophicFailureNotification, LoggingResource loggingResource,
                LoggingComponent loggingComponent)
                : base(tempPath ?? basePath, ioChangesNotifications, catastrophicFailureNotification, loggingResource, loggingComponent)
            {
                Debug.Assert(basePath != null);
                Debug.Assert(journalPath != null);

                _basePath = basePath;
                JournalPath = journalPath;

                if (Directory.Exists(_basePath.FullPath) == false)
                    Directory.CreateDirectory(_basePath.FullPath);

                if (Equals(_basePath, TempPath) == false && Directory.Exists(TempPath.FullPath) == false)
                    Directory.CreateDirectory(TempPath.FullPath);

                if (Equals(JournalPath, TempPath) == false && Directory.Exists(JournalPath.FullPath) == false)
                    Directory.CreateDirectory(JournalPath.FullPath);

                FilePath = _basePath.Combine(Constants.DatabaseFilename);

                // have to be before the journal check, so we'll fail on files in use
                DeleteAllTempFiles();

                GatherRecyclableJournalFiles(); // if there are any (e.g. after a rude db shut down) let us reuse them

                InitializePathsInfo();
            }

            private void InitializePathsInfo()
            {
                DriveInfoByPath = new LazyWithExceptionRetry<DriveInfoByPath>(() =>
                {
                    var drivesInfo = PlatformDetails.RunningOnPosix ? DriveInfo.GetDrives() : null;
                    return new DriveInfoByPath
                    {
                        BasePath = DiskUtils.GetDriveInfo(BasePath.FullPath, drivesInfo, out _),
                        JournalPath = DiskUtils.GetDriveInfo(JournalPath.FullPath, drivesInfo, out _),
                        TempPath = DiskUtils.GetDriveInfo(TempPath.FullPath, drivesInfo, out _)
                    };
                });
            }
            public VoronPathSetting FilePath { get; }

            public override (Pager Pager, Pager.State State) InitializeDataPager()
            {
                var flags = Pal.OpenFileFlags.None;
                if(Encryption.IsEnabled)
                    flags |= Pal.OpenFileFlags.Encrypted;
                if (ForceUsing32BitsPager || PlatformDetails.Is32Bits)
                    flags |= Pal.OpenFileFlags.DoNotMap;
                var result = Pager.Create(this, FilePath.FullPath,
                    InitialFileSize ?? 0,
                    flags);
                InitializeWritebackGate(result.State, FilePath.FullPath);
                return result;
            }

            public override string ToString()
            {
                return _basePath.FullPath;
            }

            public override VoronPathSetting BasePath => _basePath;

            public override void LinkFiles(long journalNumber, string filePath, out string finalFilePath)
            {
                ForTestingPurposes?.BeforeLinkFiles?.Invoke(journalNumber);

                var name = JournalName(journalNumber);
                var path = JournalPath.Combine(name);
                finalFilePath = path.FullPath;
                var rc = Pal.rvn_hard_link_non_durable(filePath, path.FullPath, out var errorCode);
                if (rc != PalFlags.FailCodes.Success)
                {
                    if (PalHelper.IsHardLinkLimitError(errorCode))
                        throw new HardLinkLimitExceededException($"Failed to link files {filePath} to {path.FullPath}. Errno: {errorCode}. The file system hard-link limit has been reached.");
                    PalHelper.ThrowLastError(rc, errorCode, $"Failed to link files {filePath} to {path.FullPath}");
                }
            }

            public override bool IsLinked(long journalNumber, string filePath, out string finalFilePath)
            {
                var name = JournalName(journalNumber);
                var path = JournalPath.Combine(name);
                finalFilePath = path.FullPath;
                if (File.Exists(path.FullPath) is false)
                    return false;

                var rc = Pal.rvn_is_same_hard_link(filePath, path.FullPath, out var isSame, out var errorCode);
                if (rc != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(rc, errorCode, $"Failed to check if files {filePath} and {path.FullPath} are the same");
                return isSame;
            }

            public override JournalWriter CreateJournalWriterForBranchEnvironment(long journalNumber, string fileName, JournalFile journalFile)
            {
                var name = JournalName(journalNumber);
                var result = _journals.GetOrAdd(name, _ =>
                    new LazyWithExceptionRetry<JournalWriter>(() => new JournalWriter(this,fileName, journalNumber, journalFile)));

                if (result.Value.Disposed)
                {
                    var newWriter = new LazyWithExceptionRetry<JournalWriter>(() => new JournalWriter(this, fileName, journalNumber, journalFile));
                    if (_journals.TryUpdate(name, newWriter, result) == false)
                        throw new InvalidOperationException("Could not update journal pager");
                    result = newWriter;
                }

                return result.Value;
            }

            public override JournalWriter CreateNewJournalWriter(long journalNumber, long minRequiredSize, long preferredSize, Pal.journal_entry journalHeaderRecord, WriteAheadJournal journal)
            {
                Volatile.Write(ref _currentJournalSizeHint, preferredSize);

                var name = JournalName(journalNumber);
                var path = JournalPath.Combine(name);

                long journalSize = preferredSize;
                if (AttemptToReuseJournal(path, minRequiredSize, preferredSize, journalNumber, journalHeaderRecord, out var reusedSize))
                {
                    journalSize = reusedSize;
                    if (reusedSize < preferredSize)
                    {
                        // next time, we want to already have a ready journal of this size
                        PrepareRecyclableJournalInBackground(preferredSize, journal);
                    }
                }
                else // nothing to reuse, we need to create a new one
                {
                    var tmp = new VoronPathSetting(Path.Combine(JournalPath.FullPath, RecyclableJournalName(Interlocked.Increment(ref _reuseCounter))));
                    using (var headerWriter = new JournalWriter(this, tmp, journalNumber, preferredSize))
                        headerWriter.WriteHeaderRecord(journalHeaderRecord);
                    MoveFileDurably(tmp.FullPath, path.FullPath);
                }

                return CreateJournalWriter(journalNumber, journalSize);
            }

            private long _currentJournalSizeHint;
            private int _journalPoolPreparationInFlight;

           
            private sealed class JournalZeroingPacingState
            {
                public WriteAheadJournal Journal;
                public int StalledMs;

                private const int MaxJournalZeroingStallMs = 500;

                [UnmanagedCallersOnly]
                public static unsafe int JournalZeroingPacing(void* state)
                {
                    var pacing = (JournalZeroingPacingState)GCHandle.FromIntPtr((IntPtr)state).Target;
                    if (pacing.Journal.IsJournalWriteActive == false)
                        return 0; // write the next chunk immediately

                    if (pacing.StalledMs >= MaxJournalZeroingStallMs)
                        return -1; // no sign of going quiet - abort, the partial file is still banked

                    pacing.StalledMs += JournalWritePipeline.RecentWriteActivityWindowMs;
                    return JournalWritePipeline.RecentWriteActivityWindowMs;
                }
            }

            public override void PrepareRecyclableJournalInBackground(long size, WriteAheadJournal journal)
            {
                if (EnableJournalPoolPrewarming == false || Disposed)
                    return;

                size = Math.Min(size, MaxLogFileSize);

                if (HasAdequateRecyclableJournal(size))
                    return;

                if (Interlocked.CompareExchange(ref _journalPoolPreparationInFlight, 1, 0) != 0)
                    return;

                EnsureJournalPoolPreparationThread();
                JournalPoolPreparationQueue.Add(new JournalPoolPreparationRequest(this, size, journal));
            }

            private sealed record JournalPoolPreparationRequest(DirectoryStorageEnvironmentOptions Options, long Size, WriteAheadJournal Journal);

            private static readonly BlockingCollection<JournalPoolPreparationRequest> JournalPoolPreparationQueue = new();
            private static Thread _journalPoolPreparationThread;

            private static void EnsureJournalPoolPreparationThread()
            {
                if (Volatile.Read(ref _journalPoolPreparationThread) != null)
                    return;

                var threadName = ThreadNames.GetNameToUse(ThreadNames.ForJournalZeroing("Voron Zero Journals"));
                var thread = new Thread(JournalPoolPreparationLoop)
                {
                    IsBackground = true,
                    Name = threadName
                };

                if (Interlocked.CompareExchange(ref _journalPoolPreparationThread, thread, null) == null)
                    thread.Start();
            }

            private static void JournalPoolPreparationLoop()
            {
                ThreadNames.AddFullThreadName(Environment.CurrentManagedThreadId, "Voron Zero Journals");

                foreach (var request in JournalPoolPreparationQueue.GetConsumingEnumerable())
                {
                    try
                    {
                        if (request.Options.Disposed == false)
                            request.Options.PrepareRecyclableJournal(request.Size, request.Journal);
                    }
                    catch (Exception ex)
                    {
                        if (request.Options._log.IsDebugEnabled)
                            request.Options._log.Debug($"Failed to prepare a recyclable journal of size {request.Size}", ex);
                    }
                    finally
                    {
                        Volatile.Write(ref request.Options._journalPoolPreparationInFlight, 0);
                    }
                }
            }

            private unsafe void PrepareRecyclableJournal(long size, WriteAheadJournal journal)
            {
                var path = Path.Combine(JournalPath.FullPath, RecyclableJournalName(Interlocked.Increment(ref _reuseCounter)));

                long zeroedBytes;
                var pacingHandle = GCHandle.Alloc(new JournalZeroingPacingState { Journal = journal });
                try
                {
                    PalFlags.FailCodes rc;
                    int errorCode;
                    using (IoMetrics.MeterIoRate(path, IoMetrics.MeterType.JournalWrite, size))
                    {
                        rc = Pal.rvn_create_zeroed_file(path, size,
                            &JournalZeroingPacingState.JournalZeroingPacing, (void*)GCHandle.ToIntPtr(pacingHandle),
                            out zeroedBytes, out errorCode);
                    }

                    if (rc != PalFlags.FailCodes.Success)
                        PalHelper.ThrowLastError(rc, errorCode, $"Failed to create a zeroed pool journal {path} of size {size}");
                }
                finally
                {
                    pacingHandle.Free();
                }

                lock (_journalsForReuse)
                {
                    if (Disposed)
                    {
                        TryDelete(path);
                        return;
                    }

                    var ticks = new FileInfo(path).LastWriteTimeUtc.Ticks;
                    while (_journalsForReuse.TryAdd(ticks, path) is false)
                        ticks++;
                }
            }

            private bool HasAdequateRecyclableJournal(long size)
            {
                lock (_journalsForReuse)
                {
                    foreach (var file in _journalsForReuse.Values)
                    {
                        try
                        {
                            if (new FileInfo(file).Length >= size)
                                return true;
                        }
                        catch (IOException)
                        {
                        }
                    }
                }

                return false;
            }

            public override JournalWriter CreateJournalWriter(long journalNumber, long journalSize)
            {
                var name = JournalName(journalNumber);
                var path = JournalPath.Combine(name);
                var result = _journals.GetOrAdd(name, _ =>
                    new LazyWithExceptionRetry<JournalWriter>(() => new JournalWriter(this, path, journalNumber, journalSize)));

                if (result.Value.Disposed)
                {
                    var newWriter = new LazyWithExceptionRetry<JournalWriter>(() => new JournalWriter(this, path, journalNumber, journalSize));
                    if (_journals.TryUpdate(name, newWriter, result) == false)
                        throw new InvalidOperationException("Could not update journal pager");
                    result = newWriter;
                }

                return result.Value;
            }

            public override JournalWriter CreateReadOnlyJournalWriter(long journalNumber, long journalSize)
            {
                var name = JournalName(journalNumber);
                var path = JournalPath.Combine(name);
                return new JournalWriter(this, path, journalNumber, journalSize, readOnlyForRecovery: true);
            }

            public override VoronPathSetting GetJournalPath(long journalNumber)
            {
                var name = JournalName(journalNumber);
                return JournalPath.Combine(name);
            }

            public override bool IsJournalHardLinked(long journalNumber)
            {
                var path = JournalPath.Combine(JournalName(journalNumber)).FullPath;
                var rc = Pal.rvn_is_hard_link(path, out var isHardLink, out var errorCode);
                if (rc != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(rc, errorCode, $"Failed to check if '{path}' is a hard link");
                return isHardLink;
            }

            protected override void Disposing()
            {
                if (Disposed)
                    return;

                Disposed = true;

                foreach (var journal in _journals)
                {
                    if (journal.Value.IsValueCreated)
                        journal.Value.Value.Dispose();
                }

                lock (_journalsForReuse)
                {
                    foreach (var reusableFile in _journalsForReuse.Values)
                    {
                        TryDelete(reusableFile);
                    }
                }
            }

            public override long? GetLatestJournalNumber()
            {
                string latestJournalName = string.Empty;
                foreach (string cur in Directory.EnumerateFiles(JournalPath.FullPath,"*.journal"))
                {
                    if(string.CompareOrdinal(latestJournalName, cur) >= 0)
                        continue;
                    latestJournalName = cur;
                }

                if (latestJournalName.Length == 0)
                    return null;
                
                return long.Parse(Path.GetFileNameWithoutExtension(latestJournalName));
            }

            public override void DeleteJournalsBelow(long journalNumber)
            {
                if (Directory.Exists(JournalPath.FullPath) == false)
                    return;

                foreach (string file in Directory.GetFiles(JournalPath.FullPath, "*.journal"))
                {
                    if (long.TryParse(Path.GetFileNameWithoutExtension(file), out var number) && number < journalNumber)
                        TryDeleteJournal(number);
                }
            }

            public override bool JournalExists(long number)
            {
                var name = JournalName(number);
                var file = JournalPath.Combine(name);
                return File.Exists(file.FullPath);
            }

            public override bool TryDeleteJournal(long number)
            {
                var name = JournalName(number);

                if (_journals.TryRemove(name, out var lazy) && lazy.IsValueCreated)
                    lazy.Value.Dispose();

                var file = JournalPath.Combine(name);
                if (File.Exists(file.FullPath) == false)
                    return false;

                if (TryRecycleJournal(file))
                    return true;

                TryDelete(file.FullPath);

                return true;
            }

            private bool TryRecycleJournal(VoronPathSetting file)
            {
                // Writing to a recycled file is safe only when we hold the last link (st_nlink == 1), when ware the sole owners
                var rc = Pal.rvn_is_hard_link(file.FullPath, out var isHardLink, out _);
                if (rc != PalFlags.FailCodes.Success || isHardLink)
                    return false;

                if (RootJournal == null)
                {
                    TryStoreJournalForReuse(file); // my own pool, I am the root
                    return true;
                }

                if (RootJournal.Env.Options is not DirectoryStorageEnvironmentOptions rootOptions ||
                    rootOptions.Disposed ||
                    CanJournalsBeLinkedWith(rootOptions) == false) // a rename cannot cross volumes
                    return false;
                    
                // we donate this to the root's pool, so it can be reused by any of its branches (or the root itself)
                rootOptions.TryStoreJournalForReuse(file);
                return true;
            }

            private static void MoveFileDurably(string src, string dst)
            {
                var rc = Pal.rvn_move_file_durable(src, dst, out var errorCode);
                if (rc != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(rc, errorCode, $"Failed to move {src} to {dst}");
            }

            private void GatherRecyclableJournalFiles()
            {
                foreach (string reusableFile in GetRecyclableJournalFiles())
                {
                    var reuseNameWithoutExt = Path.GetExtension(reusableFile.AsSpan())[1..];

                    if (long.TryParse(reuseNameWithoutExt, out var reuseNum))
                    {
                        _reuseCounter = Math.Max(_reuseCounter, reuseNum);
                    }

                    try
                    {
                        var lastWriteTimeUtcTicks = new FileInfo(reusableFile).LastWriteTimeUtc.Ticks;

                        while (_journalsForReuse.TryAdd(lastWriteTimeUtcTicks, reusableFile) is false)
                        {
                            lastWriteTimeUtcTicks++;
                        }
                    }
                    catch (Exception ex)
                    {
                        if (_log.IsDebugEnabled)
                            _log.Debug("On Storage Environment Options : Can't store journal for reuse : " + reusableFile, ex);
                        TryDelete(reusableFile);
                    }
                }
            }

            private string[] GetRecyclableJournalFiles()
            {
                try
                {
                    return Directory.GetFiles(JournalPath.FullPath, $"{RecyclableJournalFileNamePrefix}.*");
                }
                catch (Exception)
                {
                    return [];
                }
            }

            private static readonly long TicksInHour = TimeSpan.FromHours(1).Ticks;

           
            private long ComputeMaxRecyclableJournalAgeInTicks()
            {
                // _journalFillIntervalTicks has the time between the last two journal creations (the current creation pace).
                // The maximum usable age allows us to keep enough journals to cover MaxNumberOfRecyclableJournals at that rate.
                var fillInterval = Volatile.Read(ref _journalFillIntervalTicks);
                if (fillInterval <= 0)
                    return 72 * TicksInHour;

                var maxAge = fillInterval * Math.Max(1, MaxNumberOfRecyclableJournals);
                // minimum age is 15 minutes, maximum is 72 hours
                return Math.Clamp(maxAge, TicksInHour / 4, 72 * TicksInHour);
            }

            private void PruneStaleRecyclableJournals()
            {
                Debug.Assert(Monitor.IsEntered(_journalsForReuse));

                var maxAgeTicks = ComputeMaxRecyclableJournalAgeInTicks();
                var now = DateTime.UtcNow.Ticks;

                while (_journalsForReuse.Count > 1) // we want to keep the _next_ journal available
                {
                    try
                    {
                        var fileInfo = new FileInfo(_journalsForReuse.Values[0]);
                        if (fileInfo.Exists == false)
                        {
                            _journalsForReuse.RemoveAt(0);
                            continue;
                        }

                        if (now - fileInfo.LastWriteTimeUtc.Ticks > maxAgeTicks)
                        {
                            _journalsForReuse.RemoveAt(0);
                            TryDelete(fileInfo.FullName);
                            continue;
                        }
                    }
                    catch (IOException)
                    {
                        // explicitly ignoring all file errors, we don't care, we want to prune them
                        var path = _journalsForReuse.Values[0];
                        _journalsForReuse.RemoveAt(0);
                        TryDelete(path);
                        continue;
                    }

                    break; // the list is sorted by time - the first non-stale entry ends the scan
                }
            }

            private void TryStoreJournalForReuse(VoronPathSetting filename)
            {
                var reusedCount = 0;
                var reusedLimit = Math.Min(_lastReusedJournalCountOnSync, MaxNumberOfRecyclableJournals);

                try
                {
                    var fileInfo = new FileInfo(filename.FullPath);
                    if (fileInfo.Length < Volatile.Read(ref _currentJournalSizeHint))
                    {
                        // journal sizes only grow, this file can never satisfy a future request
                        TryDelete(filename.FullPath);
                        return;
                    }

                    var fileModifiedDate = fileInfo.LastWriteTimeUtc;
                    var counter = Interlocked.Increment(ref _reuseCounter);
                    var newName = Path.Combine(JournalPath.FullPath, RecyclableJournalName(counter));

                    File.Move(filename.FullPath, newName);
                    lock (_journalsForReuse)
                    {
                        PruneStaleRecyclableJournals();

                        reusedCount = _journalsForReuse.Count;

                        if (ExceededReuseLimits())
                        {
                            TryDelete(filename.FullPath);
                            return;
                        }

                        var ticks = fileModifiedDate.Ticks;

                        while (_journalsForReuse.TryAdd(ticks, newName) is false)
                            ticks++;
                    }
                }
                catch (Exception ex)
                {
                    if (_log.IsDebugEnabled)
                        _log.Debug((ExceededReuseLimits() ? "Can't remove" : "Can't store") + " journal for reuse : " + filename, ex);
                    TryDelete(filename.FullPath);
                }

                bool ExceededReuseLimits() => reusedCount >= reusedLimit;
            }

            private bool AttemptToReuseJournal(VoronPathSetting desiredPath, long minRequiredSize, long preferredSize, long journalNumber, Pal.journal_entry journalHeaderRecord, out long reusedSize)
            {
                reusedSize = 0;
                var nowTicks = DateTime.UtcNow.Ticks;
                var lastCreated = Interlocked.Exchange(ref _lastJournalCreatedTicks, nowTicks);
                if (lastCreated != 0)
                    Volatile.Write(ref _journalFillIntervalTicks, nowTicks - lastCreated);

                lock (_journalsForReuse)
                {
                    var reused = false;

                    while (_journalsForReuse.Count > 0)
                    {
                        var filename = _journalsForReuse.Values[_journalsForReuse.Count - 1];
                        _journalsForReuse.RemoveAt(_journalsForReuse.Count - 1);

                        try
                        {
                            var journalFile = new FileInfo(filename);
                            if (journalFile.Exists == false)
                                continue;

                            if (journalFile.Length > MaxLogFileSize && preferredSize <= MaxLogFileSize)
                            {
                                TryDelete(filename);
                                continue;
                            }

                            if (journalFile.Length < minRequiredSize)
                            {
                                TryDelete(filename);
                                continue;
                            }

                            using (var headerWriter = new JournalWriter(this, new VoronPathSetting(filename), journalNumber, journalFile.Length))
                            {
                                // the header record must be durable BEFORE the file gets the journal name.
                                headerWriter.WriteHeaderRecord(journalHeaderRecord);
                            }

                            MoveFileDurably(filename, desiredPath.FullPath);
                            reusedSize = journalFile.Length;
                            reused = true;
                            break;
                        }
                        catch (Exception ex)
                        {
                            TryDelete(filename);

                            if (_log.IsDebugEnabled)
                                _log.Debug("Failed to prepare " + filename + " for reuse as " + desiredPath, ex);
                        }
                    }

                    PruneStaleRecyclableJournals();

                    return reused;
                }
            }

            private void TryDelete(string file)
            {
                try
                {
                    File.Delete(file);
                }
                catch (Exception ex)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info("Failed to delete " + file, ex);
                }
            }

            public override void TryCleanupRecycledJournals()
            {
                if (Monitor.TryEnter(_journalsForReuse, 10) == false)
                    return;

                try
                {
                    foreach (var recyclableJournal in _journalsForReuse)
                    {
                        TryDelete(recyclableJournal.Value);
                    }

                    _journalsForReuse.Clear();
                }
                finally
                {
                    Monitor.Exit(_journalsForReuse);
                }
            }

            public override int GetNumberOfJournalsForReuse()
            {
                lock (_journalsForReuse)
                {
                    return _journalsForReuse.Count;
                }
            }

            public override unsafe bool ReadValidMetadata(string filename, out MetadataFile metadata)
            {
                metadata = default;
                var path = _basePath.Combine(filename);
                if (File.Exists(path.FullPath) == false)
                {
                    return false;
                }

                var fileSize = new FileInfo(path.FullPath).Length;
                using (var fs = SafeFileStream.Create(path.FullPath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, 4096, FileOptions.None))
                {
                    Span<byte> buffer = stackalloc byte[checked((int)fileSize)];
                    var totalRead = 0;
                    while (totalRead < buffer.Length)
                    {
                        var read = fs.Read(buffer[totalRead..]);
                        if (read == 0)
                            break;
                        totalRead += read;
                    }

                    ulong hash = Hashing.XXHash64.CalculateInline(buffer[sizeof(ulong)..]);
                    if (BitConverter.ToUInt64(buffer[..sizeof(ulong)]) != hash)
                        return false;

                    metadata = MemoryMarshal.Cast<byte, MetadataFile>(buffer)[0];
                    return true;
                }
            }

            public override unsafe void WriteMetadata(string filename, MetadataFile metadata)
            {
                var path = _basePath.Combine(filename);
                var rc = Pal.rvn_write_header(path.FullPath, (byte*)&metadata, sizeof(MetadataFile), out var errorCode);
                if (rc != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(rc, errorCode, $"Failed to rvn_write_header '{filename}', reason : {((PalFlags.FailCodes)rc).ToString()}");
            }

            public override bool ReadValidHeader(string filename, out FileHeader header)
            {
                header = default;
                var path = _basePath.Combine(filename);
                if (File.Exists(path.FullPath) == false)
                {
                    return false;
                }

                Span<FileHeader> headerBuf = stackalloc FileHeader[1];
                var buffer = MemoryMarshal.AsBytes(headerBuf);
                using (var fs = SafeFileStream.Create(path.FullPath, FileMode.Open, FileAccess.ReadWrite, FileShare.Read, 4096, FileOptions.None))
                {
                    var totalRead = 0;
                    while (totalRead < buffer.Length)
                    {
                        var read = fs.Read(buffer[totalRead..]);
                        if (read == 0)
                            break;
                        totalRead += read;
                    }
                    
                    // we _explicitly_ support reading less than the amount we expect
                    // to support increasing the file size in future versions

                    // We expect the size to have at least the transaction id
                    if (totalRead < FileHeader.TransactionIdOffset + sizeof(long))
                    {
                        return false;
                    }

                    int startOfHash = totalRead - sizeof(ulong);
                    ulong hash = Hashing.XXHash64.CalculateInline(buffer[..startOfHash], (ulong)headerBuf[0].TransactionId);
                    if (MemoryMarshal.TryRead(buffer[startOfHash..], out ulong expectedHash) is false ||
                        expectedHash != hash)
                        return false;

                    // handle upgrading to larger file header size, we'll zero the remainder
                    // and re-calculate the hash
                    buffer[startOfHash..].Clear();
                    
                    headerBuf[0].Hash = Hashing.XXHash64.CalculateInline(buffer[..FileHeader.HashOffset], (ulong)headerBuf[0].TransactionId);
                    
                    header = headerBuf[0];
                    return true;
                }
            }

            public override unsafe void WriteHeader(string filename, FileHeader header)
            {
                var path = _basePath.Combine(filename);
                var rc = Pal.rvn_write_header(path.FullPath, (byte*)&header, sizeof(FileHeader), out var errorCode);
                if (rc != PalFlags.FailCodes.Success)
                        PalHelper.ThrowLastError(rc, errorCode, $"Failed to rvn_write_header '{filename}', reason : {((PalFlags.FailCodes)rc).ToString()}");
            }

            public void DeleteAllTempFiles()
            {
                if (Directory.Exists(TempPath.FullPath) == false)
                    return;

                foreach (var file in Directory.GetFiles(TempPath.FullPath).Where(x => x.EndsWith(BuffersFileExtension, StringComparison.OrdinalIgnoreCase) || x.EndsWith(TempFileExtension, StringComparison.OrdinalIgnoreCase)))
                {
                    DeleteTempFile(file);
                }
            }

            private static void DeleteTempFile(string file)
            {
                const int retries = 5;
                for (int i = 0; i < retries; i++)
                {
                    try
                    {
                        File.Delete(file);
                        return;
                    }
                    catch (Exception e) when (i < retries - 1 && e is UnauthorizedAccessException or IOException)
                    {
                        // On Windows, memory-mapped file handles may not be fully released by
                        // the kernel yet even after the pager is disposed. Retry after a brief delay.
                        Thread.Sleep(50);
                    }
                }
            }

            // This is used for special pagers that are used as temp buffers and don't 
            // require encryption: compression, recovery
            public override (Pager Pager, Pager.State State) CreateTemporaryBufferPager(string name, long initialSize, bool encrypted)
            {
                // here we can afford to rename the file if needed because this is a scratch / temp
                // file that is used. We _know_ that no one expects anything from it and that 
                // the name it uses isn't _that_ important in any way, shape or form. 
                int index = 0;
                void Rename()
                {
                    var ext = Path.GetExtension(name);
                    var filename = Path.GetFileNameWithoutExtension(name);
                    name = filename + "-ren-" + (index++) + ext;
                }
                Exception err = null;
                for (int i = 0; i < 15; i++)
                {
                    var tempFile = TempPath.Combine(name);
                    try
                    {
                        if (File.Exists(tempFile.FullPath))
                            File.Delete(tempFile.FullPath);
                    }
                    catch (IOException e)
                    {
                        // this can happen if someone is holding the file, shouldn't happen
                        // but might if there is some FS caching involved where it shouldn't
                        Rename();
                        err = e;
                        continue;
                    }
                    try
                    {
                        var flags = Pal.OpenFileFlags.Temporary | Pal.OpenFileFlags.WritableMap;
                        if (ForceUsing32BitsPager || PlatformDetails.Is32Bits)
                            flags |= Pal.OpenFileFlags.DoNotMap;
                        if (encrypted)
                        {
                            flags |= Pal.OpenFileFlags.Encrypted;
                            if (Encryption.IsEnabled)
                            {
                                // if we don't need encryption here, but there is encryption, means that this is a temp buffer
                                // and we still need to ensure that this isn't paged to disk
                                flags |= Pal.OpenFileFlags.LockMemory;
                            }

                            if(DoNotConsiderMemoryLockFailureAsCatastrophicError)
                                flags|=Pal.OpenFileFlags.DoNotConsiderMemoryLockFailureAsCatastrophicError;
                        }

                        return Pager.Create(this, tempFile.FullPath, initialSize, flags);
                    }
                    catch (FileNotFoundException e)
                    {
                        // unique case, when file was previously deleted, but still exists. 
                        // This can happen on cifs mount, see RavenDB-10923
                        // if this is a temp file we can try recreate it in a different name
                        Rename();
                        err = e;
                    }
                }

                throw new InvalidOperationException("Unable to create temporary mapped file " + name + ", even after trying multiple times.", err);
            }

            public override long GetJournalFileSize(long journalNumber, JournalInfo journalInfo)
            {
                var fileInfo = GetJournalFileInfo(journalNumber, journalInfo);
                return fileInfo.Length;
            }

            public override bool CanJournalsBeLinkedWith(StorageEnvironmentOptions other)
            {
                if (ForTestingPurposes?.SimulateCannotLinkJournals == true || other.ForTestingPurposes?.SimulateCannotLinkJournals == true)
                    return false;
                return other is DirectoryStorageEnvironmentOptions &&
                       CanJournalsBeLinkedWith(other.JournalPath, JournalPath);
            }

            public override (Pager Pager, Pager.State State) OpenJournalPager(long journalNumber, JournalInfo journalInfo)
            {
                var fileInfo = GetJournalFileInfo(journalNumber, journalInfo);

                if (fileInfo.Length < InitialLogFileSize && RootJournal == null)
                {
                    EnsureMinimumSize(fileInfo);
                }

                string filename = fileInfo.FullName;
                var (pager, state) = OpenJournalPager(filename);
                if (UseSequentialReadAheadHintForJournalRecovery)
                    pager.TrySetSequentialReadAheadHint(state);
                return (pager, state);
            }

            public override (Pager Pager, Pager.State State) OpenJournalPager(string filename)
            {
                var flags = Pal.OpenFileFlags.ReadOnly;
                if (ForceUsing32BitsPager || PlatformDetails.Is32Bits)
                    flags |= Pal.OpenFileFlags.DoNotMap;
                return Pager.Create(this, filename, 0, flags, pageSize: Constants.Storage.JournalPageSize);
            }

            private FileInfo GetJournalFileInfo(long journalNumber, JournalInfo journalInfo)
            {
                var name = JournalName(journalNumber);
                var path = JournalPath.Combine(name);
                var fileInfo = new FileInfo(path.FullPath);
                if (fileInfo.Exists == false)
                    throw new InvalidJournalException(journalNumber, path.FullPath, journalInfo);
                return fileInfo;
            }

            private void EnsureMinimumSize(FileInfo fileInfo)
            {
                try
                {
                    using (var stream = fileInfo.Open(FileMode.OpenOrCreate))
                    {
                        stream.SetLength(InitialLogFileSize);
                    }
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException(
                        $"Journal file {fileInfo.FullName} could not be opened because it's size is too small and we couldn't increase it",
                        e);
                }
            }
        }

        public sealed class PureMemoryStorageEnvironmentOptions : StorageEnvironmentOptions
        {
            private readonly string _name;
            private static int _counter;

            private readonly Dictionary<string, JournalWriter> _logs = new(StringComparer.OrdinalIgnoreCase);
            private readonly HashSet<SafeFileHandle> _handles = [];
            private readonly Dictionary<string, FileHeader> _headers = new(StringComparer.OrdinalIgnoreCase);
            private MetadataFile _metadata;
            private readonly int _instanceId;


            private readonly string _filename;

            public PureMemoryStorageEnvironmentOptions(string name, VoronPathSetting tempPath,
                IoChangesNotifications ioChangesNotifications, CatastrophicFailureNotification catastrophicFailureNotification, LoggingResource loggingResource,
                LoggingComponent loggingComponent)
                : base(tempPath, ioChangesNotifications, catastrophicFailureNotification, loggingResource, loggingComponent)
            {
                _name = name;
                _instanceId = Interlocked.Increment(ref _counter);
                var guid = Guid.NewGuid();
                using (var currentProcess = Process.GetCurrentProcess())
                {

                    if (Directory.Exists(tempPath.FullPath) == false)
                        Directory.CreateDirectory(tempPath.FullPath);
                    _filename = tempPath.Combine($"ravendb-{currentProcess.Id}-{_instanceId}-data.pager-{guid}").FullPath;
                }
            }

            public override unsafe (Pager Pager, Pager.State State) InitializeDataPager()
            {
                var flags = Pal.OpenFileFlags.Temporary;
                if (Encryption.IsEnabled)
                    flags |= Pal.OpenFileFlags.Encrypted;
                if (ForceUsing32BitsPager || PlatformDetails.Is32Bits)
                    flags |= Pal.OpenFileFlags.DoNotMap;
                var (pager, state) = Pager.Create(this, _filename, InitialFileSize ?? 0, flags);
                try
                {
                    var rc = Pal.rvn_pager_get_file_handle(state.Handle, out var handle, out var error);
                    if (rc != PalFlags.FailCodes.Success)
                        PalHelper.ThrowLastError(rc, error, "Failed to get file handle for pager");
                    _handles.Add(handle);
                }
                catch
                {
                    state.Dispose();
                    pager.Dispose();
                    throw;
                }

                return (pager, state);
            }

            public override string ToString()
            {
                return "mem #" + _instanceId + " " + _name;
            }

            public override VoronPathSetting BasePath { get; } = new MemoryVoronPathSetting();

            public override bool IsLinked(long journalNumber, string filePath, out string finalFilePath)
            {
                var path = GetJournalPath(journalNumber);
                finalFilePath = path.FullPath;
                if (File.Exists(path.FullPath) is false)
                    return false;
                var rc = Pal.rvn_is_same_hard_link(filePath, path.FullPath, out var isSame, out var errorCode);
                if (rc != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(rc, errorCode, $"Failed to check if files {filePath} and {path.FullPath} are the same");

                return isSame;
            }

            public override void LinkFiles(long journalNumber, string filePath, out string finalFilePath)
            {
                ForTestingPurposes?.BeforeLinkFiles?.Invoke(journalNumber);

                var path = GetJournalPath(journalNumber);
                finalFilePath = path.FullPath;
                var rc = Pal.rvn_hard_link_non_durable(filePath, path.FullPath, out var errorCode);
                if (rc != PalFlags.FailCodes.Success)
                {
                    if (PalHelper.IsHardLinkLimitError(errorCode))
                        throw new HardLinkLimitExceededException($"Failed to link files {filePath} to {path.FullPath}. Errno: {errorCode}. The file system hard-link limit has been reached.");
                    PalHelper.ThrowLastError(rc, errorCode, $"Failed to link files {filePath} to {path.FullPath}");
                }
            }

            public override JournalWriter CreateJournalWriterForBranchEnvironment(long journalNumber, string fileName, JournalFile journalFile)
            {
                var name = JournalName(journalNumber);
                if (_logs.TryGetValue(name, out JournalWriter value))
                    return value;
                value = new JournalWriter(this, fileName, journalNumber, journalFile);

                _logs[name] = value;
                return value;
            }

            public override JournalWriter CreateJournalWriter(long journalNumber, long journalSize)
            {
                var name = JournalName(journalNumber);
                if (_logs.TryGetValue(name, out JournalWriter value))
                    return value;

                var path = GetJournalPath(journalNumber);

                value = new JournalWriter(this, path, journalNumber, journalSize, PalFlags.JournalMode.PureMemory);

                _logs[name] = value;
                return value;
            }

            public override JournalWriter CreateNewJournalWriter(long journalNumber, long minRequiredSize, long preferredSize, Pal.journal_entry journalHeaderRecord, WriteAheadJournal journal)
            {
                var writer = CreateJournalWriter(journalNumber, preferredSize);
                writer.WriteHeaderRecord(journalHeaderRecord);
                return writer;
            }

            public override JournalWriter CreateReadOnlyJournalWriter(long journalNumber, long journalSize)
            {
                throw new NotSupportedException("Pure-memory env has no hard links; CreateReadOnlyJournalWriter must not be called.");
            }

            public override VoronPathSetting GetJournalPath(long journalNumber)
            {
                var name = JournalName(journalNumber);
                using (var currentProcess = Process.GetCurrentProcess())
                {
                    var filename = $"ravendb-{currentProcess.Id}-{_instanceId}-{name}-{Guid.NewGuid()}";

                    return TempPath.Combine(filename);
                }
            }

            protected override void Disposing()
            {
                if (Disposed)
                    return;
                Disposed = true;

                foreach (SafeFileHandle handle in _handles)
                {
                    handle.Dispose();
                }
                foreach (var virtualPager in _logs)
                {
                    virtualPager.Value.Dispose();
                }

                _headers.Clear();
            }

            public override long? GetLatestJournalNumber()
            {
                string lastJournal = _logs.Keys.Order().LastOrDefault();
                if (lastJournal is null)
                    return null;
                return long.Parse(Path.GetFileNameWithoutExtension(lastJournal));
            }

            public override bool JournalExists(long number)
            {
                var name = JournalName(number);
                return _logs.ContainsKey(name);
            }

            public override bool TryDeleteJournal(long number)
            {
                var name = JournalName(number);
                if (_logs.Remove(name, out JournalWriter value) == false)
                    return false;
                value.Dispose();
                return true;
            }

            public override void DeleteJournalsBelow(long journalNumber)
            {
                foreach (var name in _logs.Keys.ToArray())
                {
                    if (long.TryParse(Path.GetFileNameWithoutExtension(name), out var number) && number < journalNumber)
                        TryDeleteJournal(number);
                }
            }

            public override bool ReadValidMetadata(string filename, out MetadataFile metadata)
            {
                if (Disposed)
                    throw new ObjectDisposedException("PureMemoryStorageEnvironmentOptions");

                metadata = _metadata;
                return _metadata.Version != 0;
            }

            public override void WriteMetadata(string filename, MetadataFile metadata)
            {
                if (Disposed)
                    throw new ObjectDisposedException("PureMemoryStorageEnvironmentOptions");

                _metadata = metadata;
            }

            public override bool ReadValidHeader(string filename, out FileHeader header)
            {
                if (Disposed)
                    throw new ObjectDisposedException("PureMemoryStorageEnvironmentOptions");
                return _headers.TryGetValue(filename, out header);
            }

            public override void WriteHeader(string filename, FileHeader header)
            {
                if (Disposed)
                    throw new ObjectDisposedException("PureMemoryStorageEnvironmentOptions");

                _headers[filename] = header;
            }

            public override (Pager Pager, Pager.State State) CreateTemporaryBufferPager(string name, long initialSize, bool encrypted)
            {
                var guid = Guid.NewGuid();
                using (var currentProcess = Process.GetCurrentProcess())
                {
                    var filename = $"ravendb-{currentProcess.Id}-{_instanceId}-{name}-{guid}";

                    var flags = Pal.OpenFileFlags.Temporary | Pal.OpenFileFlags.WritableMap;
                    if (ForceUsing32BitsPager || PlatformDetails.Is32Bits)
                        flags |= Pal.OpenFileFlags.DoNotMap;
                    if(encrypted) 
                        flags |= Pal.OpenFileFlags.Encrypted;
                    return Pager.Create(this, TempPath.Combine(filename).FullPath, initialSize, flags);
                }
            }

            public override (Pager Pager, Pager.State State) OpenJournalPager(long journalNumber, JournalInfo journalInfo)
            {
                var name = JournalName(journalNumber);
                if (_logs.TryGetValue(name, out JournalWriter value))
                    return value.CreatePager();
                throw new InvalidJournalException(journalNumber, journalInfo);
                }

            public override (Pager Pager, Pager.State State) OpenJournalPager(string name)
            {
                if (_logs.TryGetValue(name, out JournalWriter value))
                    return value.CreatePager();
                throw new InvalidJournalException(name + " was not found", null);
            }

            public override long GetJournalFileSize(long journalNumber, JournalInfo journalInfo)
            {
                var name = JournalName(journalNumber);
                if (_logs.TryGetValue(name, out JournalWriter value))
                    return new FileInfo(value.FileName.FullPath).Length;
                throw new InvalidJournalException(journalNumber, journalInfo);
            }

            public override bool CanJournalsBeLinkedWith(StorageEnvironmentOptions other)
            {
                if (ForTestingPurposes?.SimulateCannotLinkJournals == true || other.ForTestingPurposes?.SimulateCannotLinkJournals == true)
                    return false;
                return other is PureMemoryStorageEnvironmentOptions &&
                       CanJournalsBeLinkedWith(TempPath, other.TempPath);
            }
        }

        public static string JournalName(long number)
        {
            return string.Format("{0:D19}.journal", number);
        }

        public static string JournalRecoveryName(long number)
        {
            return string.Format("{0:D19}.recovery", number);
        }

        public const string RecyclableJournalFileNamePrefix = "recyclable-journal";

        public static string RecyclableJournalName(long number)
        {
            return $"{RecyclableJournalFileNamePrefix}.{number:D19}";
        }

        public int MaxNumberOfRecyclableJournals { get; set; } = 32;

        protected int _lastReusedJournalCountOnSync;

        public void SetLastReusedJournalCountOnSync(int journalNum)
        {
            _lastReusedJournalCountOnSync = journalNum;
        }

        public virtual void TryCleanupRecycledJournals()
        {
        }

        public virtual int GetNumberOfJournalsForReuse()
        {
            return 0;
        }

        public static string ScratchBufferName(long number)
        {
            return $"scratch.{number:D10}{DirectoryStorageEnvironmentOptions.BuffersFileExtension}";
        }

        public void Dispose()
        {
            NullifyHandlers();

            Encryption.Dispose();

            ScratchSpaceUsage?.Dispose();

            Disposing();
        }

        public void NullifyHandlers()
        {
            SchemaUpgrader = null;
            OnRecoveryError = null;
            OnNonDurableFileSystemError = null;
            OnIntegrityErrorOfAlreadySyncedData = null;
            OnRecoverableFailure = null;
        }

        protected abstract void Disposing();
        
        public abstract long? GetLatestJournalNumber(); 

        public abstract bool JournalExists(long number);

        public bool TryGetJournalId(string basePath, out Guid journalId)
        {
            if (ReadValidMetadata(Path.Combine(basePath, MetadataAccessor.MetadataName), out var metadata))
            {
                journalId = metadata.JournalId;
                return true;
            }

            journalId = Guid.Empty;
            return false;
        }

        public abstract bool TryDeleteJournal(long number);

        public abstract bool ReadValidMetadata(string filename, out MetadataFile metadata);
        
        public abstract void WriteMetadata(string filename, MetadataFile metadata);

        public abstract bool ReadValidHeader(string filename, out FileHeader header);

        public abstract void WriteHeader(string filename, FileHeader header);

        public abstract (Pager Pager, Pager.State State) CreateTemporaryBufferPager(string name, long initialSize, bool encrypted);

        public abstract (Pager Pager, Pager.State State) OpenJournalPager(long journalNumber, JournalInfo journalInfo);
        public abstract (Pager Pager, Pager.State State) OpenJournalPager(string name);

        public abstract long GetJournalFileSize(long journalNumber, JournalInfo journalInfo);

        public bool DoNotConsiderMemoryLockFailureAsCatastrophicError;

        public static bool RunningOnPosix
            => RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ||
               RuntimeInformation.IsOSPlatform(OSPlatform.OSX);

        public bool RunningOn32Bits => PlatformDetails.Is32Bits || ForceUsing32BitsPager;


        public PalFlags.DurabilityMode SupportDurabilityFlags { get; set; } = PalFlags.DurabilityMode.DurabililtySupported;

        public TimeSpan DisposeWaitTime { get; set; }

        public int TimeToSyncAfterFlushInSec
        {
            get
            {
                if (_timeToSyncAfterFlushInSec < 1)
                    _timeToSyncAfterFlushInSec = 30;
                return _timeToSyncAfterFlushInSec;
            }
            set => _timeToSyncAfterFlushInSec = value;
        }

        public long PrefetchSegmentSize { get; set; }
        public long PrefetchResetThreshold { get; set; }
        public long SyncJournalsCountThreshold { get; set; }

        public long MaxUnsyncedBytesBeforeSync { get; set; } = 256 * Constants.Size.Megabyte;

        public long MaxUnsyncedBytesBeforeMandatorySync { get; set; } = 768L * Constants.Size.Megabyte;

        public int SyncWritebackBlockSizeInMb { get; set; } = 32;

        public int SyncWritebackMinContiguousSizeInKb { get; set; } = 64;

        public int SyncWritebackBarrierCostThresholdInMs { get; set; } = 100;

        internal long SyncWritebackBarrierCostThresholdTicks => SyncWritebackBarrierCostThresholdInMs * TimeSpan.TicksPerMillisecond;

        public int SyncWritebackDrainQueueDepthThreshold { get; set; } = 5;

        public const int MaxSupportedConcurrentJournalWrites = 64;

        public long PipelineJournalWritesAboveLatencyInTicks { get; set; } = 2 * TimeSpan.TicksPerMillisecond;

        private int _maxConcurrentJournalWrites = 4;

        public int MaxConcurrentJournalWrites
        {
            get => _maxConcurrentJournalWrites;
            set => _maxConcurrentJournalWrites = Math.Clamp(value, 1, MaxSupportedConcurrentJournalWrites);
        }

        internal WritebackPacingGate WritebackGate { get; private set; }

        private protected unsafe void InitializeWritebackGate(Pager.State state, string dataFilePath)
        {
            if (Pal.rvn_pager_get_device_id(state.Handle, out var deviceId, out _) != PalFlags.FailCodes.Success)
                return;
                
            WritebackGate = WritebackPacingGate.GetForDevice(deviceId, dataFilePath,
                SyncWritebackBarrierCostThresholdTicks, SyncWritebackDrainQueueDepthThreshold);
        }

        internal bool SimulateFailureOnDbCreation { get; set; }
        internal bool ManualSyncing { get; set; } = false;
        public bool? IgnoreInvalidJournalErrors { get; set; }
        public bool IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions { get; set; }
        public bool SkipChecksumValidationOnDatabaseLoading { get; set; }
        public bool DiscardVirtualMemory { get; set; } = true;
        public bool DisableSparseRegions { get; set; }
        public int JournalsCompressionAcceleration { get; set; } = 1;

        public JournalCompressionAlgorithm JournalCompressionAlgorithm { get; set; } = JournalCompressionAlgorithm.Lz4;
        public int MinimumSharedJournalsMergeCount { get; set; } = 8;
        public bool UseSequentialReadAheadHintForJournalRecovery { get; set; } = true;

        private readonly RavenLogger _log;

        private int _timeToSyncAfterFlushInSec;
        public long CompressTxAboveSizeInBytes;
        private Guid _environmentId;
        private long _maxScratchBufferSize;



        public void SetEnvironmentId(Guid environmentId)
        {
            _environmentId = environmentId;
        }

        public void InvokeOnDirectoryInitialize()
        {
            OnDirectoryInitialize?.Invoke(this);
        }

        public void SetDurability()
        {
            if (BasePath != null)
            {
                string testFile = Path.Combine(BasePath.FullPath, "test-" + Guid.NewGuid() + ".tmp");
                var rc = Pal.rvn_test_storage_durability(testFile, out var errorCode);
                switch (rc)
                {
                    case PalFlags.FailCodes.FailOpenFile:
                        {
                            if (_log.IsDebugEnabled)
                                _log.Debug(
                                    $"Failed to create test file at '{testFile}'. Error:'{PalHelper.GetNativeErrorString(errorCode, "Failed to open test file", out _)}'. Cannot determine if O_DIRECT supported by the file system. Assuming it is");
                        }
                        break;

                    case PalFlags.FailCodes.FailAllocFile:
                        {
                            if (_log.IsDebugEnabled)
                                _log.Debug(
                                    $"Failed to allocate test file at '{testFile}'. Error:'{PalHelper.GetNativeErrorString(errorCode, "Failed to allocate space for test file", out _)}'. Cannot determine if O_DIRECT supported by the file system. Assuming it is");
                        }
                        break;

                    case PalFlags.FailCodes.FailTestDurability:
                        {
                            SupportDurabilityFlags = PalFlags.DurabilityMode.DurabilityNotSupported;

                            var message = "Path " + BasePath +
                                          " not supporting O_DIRECT writes. As a result - data durability is not guaranteed";
                            var details =
                                $"Storage type '{PosixHelper.GetFileSystemOfPath(BasePath.FullPath)}' doesn't support direct write to disk (non durable file system)";
                            InvokeNonDurableFileSystemError(this, message, new NonDurableFileSystemException(message), details);
                        }
                        break;
                    case PalFlags.FailCodes.Success:
                        break;
                    default:
                        if (_log.IsDebugEnabled)
                            _log.Debug(
                                $"Unknown failure on test file at '{testFile}'. Error:'{PalHelper.GetNativeErrorString(errorCode, "Unknown error while testing O_DIRECT", out _)}'. Cannot determine if O_DIRECT supported by the file system. Assuming it is");
                        break;
                }
            }
        }

        public sealed class StorageEncryptionOptions : IDisposable
        {
            private WriteAheadJournal _journalCompressionBufferHandler;

            public WriteAheadJournal WriteAheadJournal
            {
                get
                {
                    if (HasExternalJournalCompressionBufferHandlerRegistration == false)
                        throw new InvalidOperationException($"You have to {nameof(RegisterForJournalCompressionHandler)} before you try to access {nameof(WriteAheadJournal)}");
                    
                    return _journalCompressionBufferHandler;
                }
                private set => _journalCompressionBufferHandler = value;
            }

            public byte[] MasterKey;

            public bool IsEnabled => MasterKey != null;

            public EncryptionBuffersPool EncryptionBuffersPool = EncryptionBuffersPool.Instance;

            public bool HasExternalJournalCompressionBufferHandlerRegistration { get; private set; }

            public void RegisterForJournalCompressionHandler()
            {
                if (IsEnabled == false)
                    return;

                HasExternalJournalCompressionBufferHandlerRegistration = true;
            }

            public void SetExternalCompressionBufferHandler(WriteAheadJournal handler)
            {
                WriteAheadJournal = handler;
            }

            public unsafe void Dispose()
            {
                var copy = MasterKey;
                if (copy != null)
                {
                    fixed (byte* key = copy)
                    {
                        Sodium.sodium_memzero(key, (UIntPtr)copy.Length);
                        MasterKey = null;
                    }
                }
            }
        }

        /// <summary>
        /// This is used when we have a branch environment, whose journal
        /// is actually managed by a root environment
        /// </summary>
        public WriteAheadJournal RootJournal;

        public abstract bool CanJournalsBeLinkedWith(StorageEnvironmentOptions other);
        
        protected static bool CanJournalsBeLinkedWith(VoronPathSetting otherPath, VoronPathSetting selfPath)
        {
            string fileName = Guid.NewGuid() + ".test-hard-link";
            string src = otherPath.Combine(fileName).FullPath;
            string dst = selfPath.Combine(fileName).FullPath;
            File.WriteAllText(src, "This file was created to see if hard links between document database & index work");
            var rc = Pal.rvn_hard_link_non_durable(src,dst,out _);
              
            File.Delete(src);
            if (rc != PalFlags.FailCodes.Success)
                return false;
                
            File.Delete(dst);
            return true;
        }

        internal TestingStuff ForTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (ForTestingPurposes != null)
                return ForTestingPurposes;

            return ForTestingPurposes = new TestingStuff();
        }

        internal sealed class TestingStuff
        {
            internal Action<long> BeforeLinkFiles;

            internal bool SimulateCannotLinkJournals;

            internal Func<long, PartialJournalWriteFailure> SimulatePartialJournalWriteFailure;

            internal Action<long, long> OnJournalWrite;

            internal Action<long, long> OnJournalWriteCompleted;

            internal sealed class PartialJournalWriteFailure
            {
                public long NumberOf4KbsToWrite;
                public Exception Error;
            }
        }
    }
}
