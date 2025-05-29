using System;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Server.Config.Categories;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Server.Logging;
using Sparrow.Server.Utils;
using Voron;
using Voron.Impl.Journal;

namespace Raven.Server.Documents.Indexes;

public class SharedIndexJournals : IJournalMerger, IDisposable
{
    private readonly DocumentDatabase _documentDatabase;

    public SharedIndexJournals(DocumentDatabase documentDatabase)
    {
        _documentDatabase = documentDatabase;
        string sharedJournalsPath = documentDatabase.Configuration.Indexing.SharedJournalsPath.FullPath;
        string documentDatabaseName = $"{documentDatabase.Name}.Indexes.{IndexingConfiguration.SharedJournalsStorageName}";
        var options = documentDatabase.Configuration.Indexing.RunInMemory
            ? StorageEnvironmentOptions.CreateMemoryOnly(sharedJournalsPath, Path.Combine(sharedJournalsPath, "Temp"),
                documentDatabase.IoChanges, documentDatabase.CatastrophicFailureNotification, LoggingResource.Database(documentDatabaseName),
                LoggingComponent.Name(documentDatabaseName))
            : StorageEnvironmentOptions.ForPath(sharedJournalsPath, Path.Combine(sharedJournalsPath, "Temp"), null,
                documentDatabase.IoChanges, documentDatabase.CatastrophicFailureNotification, LoggingResource.Database(documentDatabaseName),
                LoggingComponent.Name(documentDatabaseName));
        
        options.CompressTxAboveSizeInBytes = documentDatabase.Configuration.Storage.CompressTxAboveSize.GetValue(SizeUnit.Bytes);
        options.ForceUsing32BitsPager = documentDatabase.Configuration.Storage.ForceUsing32BitsPager;
        options.EnablePrefetching = documentDatabase.Configuration.Storage.EnablePrefetching;
        options.DiscardVirtualMemory = documentDatabase.Configuration.Storage.DiscardVirtualMemory;
        options.TimeToSyncAfterFlushInSec = (int)documentDatabase.Configuration.Storage.TimeToSyncAfterFlush.AsTimeSpan.TotalSeconds;
        options.AddToInitLog = documentDatabase.AddToInitLog;
        options.Encryption.MasterKey = documentDatabase.MasterKey?.ToArray();
        options.Encryption.RegisterForJournalCompressionHandler();
        options.DoNotConsiderMemoryLockFailureAsCatastrophicError = documentDatabase.Configuration.Security.DoNotConsiderMemoryLockFailureAsCatastrophicError;
        if (documentDatabase.Configuration.Storage.MaxScratchBufferSize.HasValue)
            options.MaxScratchBufferSize = documentDatabase.Configuration.Storage.MaxScratchBufferSize.Value.GetValue(SizeUnit.Bytes);
        options.PrefetchSegmentSize = documentDatabase.Configuration.Storage.PrefetchBatchSize.GetValue(SizeUnit.Bytes);
        options.PrefetchResetThreshold = documentDatabase.Configuration.Storage.PrefetchResetThreshold.GetValue(SizeUnit.Bytes);
        options.SyncJournalsCountThreshold = documentDatabase.Configuration.Storage.SyncJournalsCountThreshold;
        options.IgnoreInvalidJournalErrors = documentDatabase.Configuration.Storage.IgnoreInvalidJournalErrors;
        options.SkipChecksumValidationOnDatabaseLoading = documentDatabase.Configuration.Storage.SkipChecksumValidationOnDatabaseLoading;
        options.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions = documentDatabase.Configuration.Storage.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions;
        options.DisableSparseRegions = documentDatabase.Configuration.Storage.DisableSparseRegions;
        options.JournalsCompressionAcceleration = documentDatabase.Configuration.Storage.JournalsCompressionAcceleration;
        options.MinimumSharedJournalsMergeCount = documentDatabase.Configuration.Storage.MinimumSharedJournalsMergeCount;
        options.MaxLogFileSize = documentDatabase.Configuration.Storage.MaxJournalFileSize.GetValue(SizeUnit.Bytes);

        options.OnNonDurableFileSystemError += documentDatabase.HandleNonDurableFileSystemError;
        options.OnRecoveryError += (s, e) => documentDatabase.HandleOnIndexRecoveryError(IndexingConfiguration.SharedJournalsStorageName, s, e);
        options.OnIntegrityErrorOfAlreadySyncedData += (s, e) => documentDatabase.HandleOnIndexIntegrityErrorOfAlreadySyncedData(IndexingConfiguration.SharedJournalsStorageName, s, e);
        options.OnRecoverableFailure += documentDatabase.HandleRecoverableFailure;

        _env = new StorageEnvironment(options);
        _env.Journal.BranchJournalMerger = this;
        _scopeForSharedJournals = _env.Journal.SharedJournalsScope();

        _sharedJournalsThread = PoolOfThreads.GlobalRavenThreadPool.LongRunning(
            WriteSharedJournals, null,
            ThreadNames.ForIndexSharedJournals("Index SharedJournals for " + documentDatabase.Name, documentDatabase.Name));
    }

    
    private readonly ManualResetEventSlim _waitForJournals = new(initialState: true);
    private readonly StorageEnvironment _env;
    public StorageEnvironment Env => _env;
    private bool _disposed;
    private readonly PoolOfThreads.LongRunningWork _sharedJournalsThread;
    private readonly WriteAheadJournal.ScopeForSharedJournals _scopeForSharedJournals;

    private void WriteSharedJournals(object _)
    {
        using (_scopeForSharedJournals)
        {
            while (_disposed is false)
            {
                try
                {

                    _waitForJournals.Wait();
                    _waitForJournals.Reset();
                    do
                    {
                        var curJournal = _env.Journal.CurrentFile;
                        using (var txw = _env.WriteTransaction())
                        {
                            txw.Commit();
                        }

                        if (curJournal == _env.Journal.CurrentFile)
                            continue;

                        // this will force us to do an actual commit
                        // to our own journal, and thus force us to 
                        // flush the journals, etc...
                        // 
                        // This is required to ensure that journals are properly
                        // flushed & handled after we switch between journals
                        using (var txw = _env.WriteTransaction())
                        {
                            // we do a dummy change here to force the env
                            // to think that it has an actual transaction and thus
                            // will force it to flush / remove older journal
                            txw.LowLevelTransaction.ModifyPage(0);
                            txw.Commit();
                        }
                    } while (_env.Journal.HasBranchCommits);
                }
                catch (Exception e)
                {
                    Interlocked.Exchange(ref _env.Journal.SharedJournalState, new SharedJournalState()).SetException(e);
                }
            }
        }
    }

    public void JournalMergeSubmitted()
    {
        _waitForJournals.Set();
    }

    public void Dispose()
    {
        _disposed = true;
        _waitForJournals.Set();
        _sharedJournalsThread.Join(Timeout.Infinite);
        _waitForJournals.Dispose();
        _env.Dispose();
    }

    public void Register(StorageEnvironmentOptions branchOptions)
    {
        branchOptions.RootJournal = _env.Journal;
    }
}
