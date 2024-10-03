﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Eventing.Reader;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Mono.Unix.Native;
using Sparrow;
using Sparrow.Collections;
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
using Voron.Platform.Win32;
using Voron.Util;
using Voron.Util.Settings;
using Constants = Voron.Global.Constants;
using NativeMemory = Sparrow.Utils.NativeMemory;

namespace Voron
{
    public abstract class StorageEnvironmentOptions : IDisposable
    {
        public const string RecyclableJournalFileNamePrefix = "recyclable-journal";

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

        private long _reuseCounter;
        private long _lastReusedJournalCountOnSync;

        public void SetLastReusedJournalCountOnSync(long journalNum)
        {
            _lastReusedJournalCountOnSync = journalNum;
        }

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
            OnRecoveryError = null;

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

            handler(this, new RecoveryErrorEventArgs(message, e));
        }

        internal DisposableAction DisableOnIntegrityErrorOfAlreadySyncedDataHandler()
        {
            var handler = OnIntegrityErrorOfAlreadySyncedData;
            OnIntegrityErrorOfAlreadySyncedData = null;

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

        public abstract JournalWriter CreateJournalWriter(long journalNumber, long journalSize);

        public abstract VoronPathSetting GetJournalPath(long journalNumber);

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
                if (_log.IsInfoEnabled)
                    _log.Info($"Recoverable failure in {this}. Error: {failureMessage}.", e);
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
                if (_log.IsInfoEnabled)
                    _log.Info($"CatastrophicFailure state, about to throw. Originally was set in the following stack trace : {_catastrophicFailureStack}");

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

            private void GatherRecyclableJournalFiles()
            {
                foreach (var reusableFile in GetRecyclableJournalFiles())
                {
                    var reuseNameWithoutExt = Path.GetExtension(reusableFile).Substring(1);

                    long reuseNum;
                    if (long.TryParse(reuseNameWithoutExt, out reuseNum))
                    {
                        _reuseCounter = Math.Max(_reuseCounter, reuseNum);
                    }

                    try
                    {
                        var lastWriteTimeUtcTicks = new FileInfo(reusableFile).LastWriteTimeUtc.Ticks;

                        while (_journalsForReuse.ContainsKey(lastWriteTimeUtcTicks))
                        {
                            lastWriteTimeUtcTicks++;
                        }

                        _journalsForReuse[lastWriteTimeUtcTicks] = reusableFile;
                    }
                    catch (Exception ex)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info("On Storage Environment Options : Can't store journal for reuse : " + reusableFile, ex);
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

            public VoronPathSetting FilePath { get; }

            public override (Pager Pager, Pager.State State) InitializeDataPager()
            {
                var flags = Pal.OpenFileFlags.None;
                if(Encryption.IsEnabled)
                    flags |= Pal.OpenFileFlags.Encrypted;
                if (ForceUsing32BitsPager || PlatformDetails.Is32Bits)
                    flags |= Pal.OpenFileFlags.DoNotMap;
                return Pager.Create(this, FilePath.FullPath,
                    InitialFileSize ?? 0,
                    flags);
            }

            public override string ToString()
            {
                return _basePath.FullPath;
            }

            public override VoronPathSetting BasePath => _basePath;

            public override JournalWriter CreateJournalWriter(long journalNumber, long journalSize)
            {
                var name = JournalName(journalNumber);
                var path = JournalPath.Combine(name);
                if (File.Exists(path.FullPath) == false)
                    AttemptToReuseJournal(path, journalSize);

                var result = _journals.GetOrAdd(name, _ =>
                    new LazyWithExceptionRetry<JournalWriter>(() => new JournalWriter(this, path, journalSize)));

                if (result.Value.Disposed)
                {
                    var newWriter = new LazyWithExceptionRetry<JournalWriter>(() => new JournalWriter(this, path, journalSize));
                    if (_journals.TryUpdate(name, newWriter, result) == false)
                        throw new InvalidOperationException("Could not update journal pager");
                    result = newWriter;
                }

                return result.Value;
            }

            public override VoronPathSetting GetJournalPath(long journalNumber)
            {
                var name = JournalName(journalNumber);
                return JournalPath.Combine(name);
            }

            private static readonly long TickInHour = TimeSpan.FromHours(1).Ticks;

            public override void TryStoreJournalForReuse(VoronPathSetting filename)
            {
                var reusedCount = 0;
                var reusedLimit = Math.Min(_lastReusedJournalCountOnSync, MaxNumberOfRecyclableJournals);

                try
                {
                    var oldFileName = Path.GetFileName(filename.FullPath);
                    _journals.TryRemove(oldFileName, out _);

                    var fileModifiedDate = new FileInfo(filename.FullPath).LastWriteTimeUtc;
                    var counter = Interlocked.Increment(ref _reuseCounter);
                    var newName = Path.Combine(Path.GetDirectoryName(filename.FullPath), RecyclableJournalName(counter));
                    
                    File.Move(filename.FullPath, newName);
                    lock (_journalsForReuse)
                    {
                        reusedCount = _journalsForReuse.Count;

                        if (ShouldRemoveJournal())
                        {
                            if (File.Exists(newName))
                                File.Delete(newName);
                            return;
                        }

                        var ticks = fileModifiedDate.Ticks;

                        while (_journalsForReuse.ContainsKey(ticks))
                            ticks++;

                        _journalsForReuse[ticks] = newName;
                        
                    }
                }
                catch (Exception ex)
                {
                    if (_log.IsInfoEnabled)
                        _log.Info(ShouldRemoveJournal() ? "Can't remove" : "Can't store" + " journal for reuse : " + filename, ex);
                    try
                    {
                        if (File.Exists(filename.FullPath))
                            File.Delete(filename.FullPath);
                    }
                    catch
                    {
                        // nothing we can do about it
                    }
                }

                bool ShouldRemoveJournal()
                {
                    return reusedCount >= reusedLimit;
                }
            }

            public override int GetNumberOfJournalsForReuse()
            {
                return _journalsForReuse.Count;
            }

            private void AttemptToReuseJournal(VoronPathSetting desiredPath, long desiredSize)
            {
                lock (_journalsForReuse)
                {
                    var lastModified = DateTime.MinValue.Ticks;
                    while (_journalsForReuse.Count > 0)
                    {
                        lastModified = _journalsForReuse.Keys[_journalsForReuse.Count - 1];
                        var filename = _journalsForReuse.Values[_journalsForReuse.Count - 1];
                        _journalsForReuse.RemoveAt(_journalsForReuse.Count - 1);

                        try
                        {
                            var journalFile = new FileInfo(filename);
                            if (journalFile.Exists == false)
                                continue;

                            if (journalFile.Length > MaxLogFileSize && desiredSize <= MaxLogFileSize)
                            {
                                // delete journals that are bigger than MaxLogFileSize when tx desiredSize is smaller than MaxLogFileSize
                                TryDelete(filename);
                                continue;
                            }

                            journalFile.MoveTo(desiredPath.FullPath);
                            break;
                        }
                        catch (Exception ex)
                        {
                            TryDelete(filename);

                            if (_log.IsInfoEnabled)
                                _log.Info("Failed to rename " + filename + " to " + desiredPath, ex);
                        }
                    }

                    while (_journalsForReuse.Count > 0)
                    {
                        try
                        {
                            var fileInfo = new FileInfo(_journalsForReuse.Values[0]);
                            if (fileInfo.Exists == false)
                            {
                                _journalsForReuse.RemoveAt(0);
                                continue;
                            }

                            if (lastModified - fileInfo.LastWriteTimeUtc.Ticks > TickInHour * 72)
                            {
                                _journalsForReuse.RemoveAt(0);
                                TryDelete(fileInfo.FullName);
                                continue;
                            }

                            if (fileInfo.Length < desiredSize)
                            {
                                _journalsForReuse.RemoveAt(0);
                                TryDelete(fileInfo.FullName);

                                continue;
                            }

                            if (fileInfo.Length > MaxLogFileSize && desiredSize <= MaxLogFileSize)
                            {
                                _journalsForReuse.RemoveAt(0);
                                TryDelete(fileInfo.FullName);

                                continue;
                            }
                        }
                        catch (IOException)
                        {
                            // explicitly ignoring any such file errors
                            _journalsForReuse.RemoveAt(0);
                            TryDelete(_journalsForReuse.Values[0]);
                        }
                        break;
                    }

                }
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

                File.Delete(file.FullPath);

                return true;
            }

            public override unsafe bool ReadHeader(string filename, FileHeader* header)
            {
                var path = _basePath.Combine(filename);
                if (File.Exists(path.FullPath) == false)
                {
                    return false;
                }

                var success = RunningOnPosix ?
                    PosixHelper.TryReadFileHeader(header, path) :
                    Win32Helper.TryReadFileHeader(header, path);

                if (!success)
                    return false;

                return header->Hash == HeaderAccessor.CalculateFileHeaderHash(header);
            }


            public override unsafe void WriteHeader(string filename, FileHeader* header)
            {
                var path = _basePath.Combine(filename);
                var rc = Pal.rvn_write_header(path.FullPath, header, sizeof(FileHeader), out var errorCode);
                if (rc != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(rc, errorCode, $"Failed to rvn_write_header '{filename}', reason : {((PalFlags.FailCodes)rc).ToString()}");
            }

            public void DeleteAllTempFiles()
            {
                if (Directory.Exists(TempPath.FullPath) == false)
                    return;

                foreach (var file in Directory.GetFiles(TempPath.FullPath).Where(x => x.EndsWith(BuffersFileExtension, StringComparison.OrdinalIgnoreCase) || x.EndsWith(TempFileExtension, StringComparison.OrdinalIgnoreCase)))
                {
                    File.Delete(file);
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
                                flags|=Pal.OpenFileFlags.LockMemory;
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

            public override (Pager Pager, Pager.State State) OpenJournalPager(long journalNumber, JournalInfo journalInfo)
            {
                var fileInfo = GetJournalFileInfo(journalNumber, journalInfo);

                if (fileInfo.Length < InitialLogFileSize)
                {
                    EnsureMinimumSize(fileInfo);
                }

                string filename = fileInfo.FullName;
                return OpenJournalPager(filename);
            }

            public override (Pager Pager, Pager.State State) OpenJournalPager(string filename)
            {
                var flags = Pal.OpenFileFlags.ReadOnly;
                if (ForceUsing32BitsPager || PlatformDetails.Is32Bits)
                    flags |= Pal.OpenFileFlags.DoNotMap;
                return Pager.Create(this, filename, 0, flags);
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
            private readonly Dictionary<string, IntPtr> _headers = new(StringComparer.OrdinalIgnoreCase);
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
                if(Encryption.IsEnabled)
                    flags |= Pal.OpenFileFlags.Encrypted;
                if (ForceUsing32BitsPager || PlatformDetails.Is32Bits)
                    flags |= Pal.OpenFileFlags.DoNotMap;
                var (pager,state) = Pager.Create(this, _filename, InitialFileSize ?? 0, flags);
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
                return (pager,state);
            }

            public override string ToString()
            {
                return "mem #" + _instanceId + " " + _name;
            }

            public override VoronPathSetting BasePath { get; } = new MemoryVoronPathSetting();

            public override JournalWriter CreateJournalWriter(long journalNumber, long journalSize)
            {
                var name = JournalName(journalNumber);
                if (_logs.TryGetValue(name, out JournalWriter value))
                    return value;

                var path = GetJournalPath(journalNumber);

                value = new JournalWriter(this, path, journalSize, PalFlags.JournalMode.PureMemory);

                _logs[name] = value;
                return value;
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

            public override void TryStoreJournalForReuse(VoronPathSetting filename)
            {
            }

            public override int GetNumberOfJournalsForReuse()
            {
                return 0;
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

                foreach (var headerSpace in _headers)
                {
                    Marshal.FreeHGlobal(headerSpace.Value);
                }

                _headers.Clear();
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

            public override unsafe bool ReadHeader(string filename, FileHeader* header)
            {
                if (Disposed)
                    throw new ObjectDisposedException("PureMemoryStorageEnvironmentOptions");
                IntPtr ptr;
                if (_headers.TryGetValue(filename, out ptr) == false)
                {
                    return false;
                }
                *header = *((FileHeader*)ptr);

                return header->Hash == HeaderAccessor.CalculateFileHeaderHash(header);
            }

            public override unsafe void WriteHeader(string filename, FileHeader* header)
            {
                if (Disposed)
                    throw new ObjectDisposedException("PureMemoryStorageEnvironmentOptions");

                IntPtr ptr;
                if (_headers.TryGetValue(filename, out ptr) == false)
                {
                    ptr = (IntPtr)NativeMemory.AllocateMemory(sizeof(FileHeader));
                    _headers[filename] = ptr;
                }
                Memory.Copy((byte*)ptr, (byte*)header, sizeof(FileHeader));
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
        }

        public static string JournalName(long number)
        {
            return string.Format("{0:D19}.journal", number);
        }

        public static string RecyclableJournalName(long number)
        {
            return $"{RecyclableJournalFileNamePrefix}.{number:D19}";
        }

        public static string JournalRecoveryName(long number)
        {
            return string.Format("{0:D19}.recovery", number);
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

        public abstract bool JournalExists(long number);

        public abstract bool TryDeleteJournal(long number);

        public abstract unsafe bool ReadHeader(string filename, FileHeader* header);

        public abstract unsafe void WriteHeader(string filename, FileHeader* header);

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

        internal bool SimulateFailureOnDbCreation { get; set; }
        internal bool ManualSyncing { get; set; } = false;
        public bool? IgnoreInvalidJournalErrors { get; set; }
        public bool IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions { get; set; }
        public bool SkipChecksumValidationOnDatabaseLoading { get; set; }

        public int MaxNumberOfRecyclableJournals { get; set; } = 32;
        public bool DiscardVirtualMemory { get; set; } = true;
        
        private readonly RavenLogger _log;

        private readonly SortedList<long, string> _journalsForReuse = new SortedList<long, string>();

        private int _timeToSyncAfterFlushInSec;
        public long CompressTxAboveSizeInBytes;
        private Guid _environmentId;
        private long _maxScratchBufferSize;

        public abstract void TryStoreJournalForReuse(VoronPathSetting filename);

        public abstract int GetNumberOfJournalsForReuse();

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

        public void TryCleanupRecycledJournals()
        {
            if (Monitor.TryEnter(_journalsForReuse, 10) == false)
                return;

            try
            {
                foreach (var recyclableJournal in _journalsForReuse)
                {
                    try
                    {
                        var fileInfo = new FileInfo(recyclableJournal.Value);

                        if (fileInfo.Exists)
                            TryDelete(fileInfo.FullName);
                    }
                    catch (Exception ex)
                    {
                        if (_log.IsInfoEnabled)
                            _log.Info($"Couldn't delete recyclable journal: {recyclableJournal.Value}", ex);
                    }
                }

                _journalsForReuse.Clear();
            }
            finally
            {
                Monitor.Exit(_journalsForReuse);
            }
        }

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
                            if (_log.IsInfoEnabled)
                                _log.Info(
                                    $"Failed to create test file at '{testFile}'. Error:'{PalHelper.GetNativeErrorString(errorCode, "Failed to open test file", out _)}'. Cannot determine if O_DIRECT supported by the file system. Assuming it is");
                        }
                        break;

                    case PalFlags.FailCodes.FailAllocFile:
                        {
                            if (_log.IsInfoEnabled)
                                _log.Info(
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
                        if (_log.IsInfoEnabled)
                            _log.Info(
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

        internal TestingStuff ForTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (ForTestingPurposes != null)
                return ForTestingPurposes;

            return ForTestingPurposes = new TestingStuff();
        }

        internal sealed class TestingStuff
        {
            public int? WriteToJournalCompressionAcceleration = null;
        }
    }
}
