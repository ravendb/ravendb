using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Server.Config.Categories;
using Raven.Server.Logging;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.Storage.Layout;
using Raven.Server.Utils;
using Sparrow.Logging;
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
        
        options.AddToInitLog = documentDatabase.AddToInitLog;
        options.Encryption.MasterKey = documentDatabase.MasterKey?.ToArray();
        options.Encryption.RegisterForJournalCompressionHandler();
        VoronOptionsFromConfiguration.Apply(options, documentDatabase.Configuration);

        options.OnNonDurableFileSystemError += documentDatabase.HandleNonDurableFileSystemError;
        options.OnRecoveryError += (s, e) => documentDatabase.HandleOnIndexRecoveryError(IndexingConfiguration.SharedJournalsStorageName, s, e);
        options.OnIntegrityErrorOfAlreadySyncedData += (s, e) => documentDatabase.HandleOnIndexIntegrityErrorOfAlreadySyncedData(IndexingConfiguration.SharedJournalsStorageName, s, e);
        options.OnRecoverableFailure += documentDatabase.HandleRecoverableFailure;

        _env = StorageLoader.OpenEnvironment(options, StorageEnvironmentWithType.StorageEnvironmentType.SharedJournals);
        _logger = RavenLogManager.Instance.GetLoggerForDatabase<SharedIndexJournals>(documentDatabase);
        _env.Journal.BranchJournalMerger = this;
        _env.Journal.OnBranchHardLinkLimitReached = OnBranchHardLinkLimitReached;
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
    private readonly RavenLogger _logger;

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

    private readonly ConcurrentDictionary<StorageEnvironment, DateTime> _fallbacks = new(ReferenceEqualityComparer.Instance);

    // index environments that switched to their own journals since the database started, and when
    public IReadOnlyDictionary<StorageEnvironment, DateTime> Fallbacks => _fallbacks;

    private void OnBranchHardLinkLimitReached(StorageEnvironment branchEnv)
    {
        DateTime at = DateTime.UtcNow;
        _fallbacks[branchEnv] = at;

        if (_logger.IsWarnEnabled)
            _logger.Warn($"Index environment at '{branchEnv.Options.BasePath}' exceeded the file system hard-link limit and switched to unshared journal mode. Subsequent journal writes for this index will go to its own Journals directory.");

        // we are on the shared journal writer, inside its write: raising the alert writes to the notification storage, so do it elsewhere
        ThreadPool.UnsafeQueueUserWorkItem(_ => RaiseFallbackAlert(branchEnv, at), null);
    }

    private void RaiseFallbackAlert(StorageEnvironment branchEnv, DateTime at)
    {
        try
        {
            string index = _documentDatabase.IndexStore?.GetIndexes().FirstOrDefault(i => i._indexStorage?.Environment() == branchEnv)?.Name ?? branchEnv.Options.BasePath.FullPath;

            _documentDatabase.NotificationCenter.Add(AlertRaised.Create(
                _documentDatabase.Name,
                $"Index '{index}' writes its own journals",
                $"Since {at:u} the index '{index}' writes its journal to its own files instead of the database's shared index journal, because the file system limit " +
                "of hard links per file was reached. Indexing keeps working, but this index now pays for its own journal writes and disk space. " +
                "The index rejoins the shared journal after the database restarts.",
                AlertReason.Indexing_SharedJournalsFallback,
                NotificationSeverity.Warning,
                key: index));
        }
        catch (Exception e)
        {
            if (_logger.IsInfoEnabled)
                _logger.Info($"Failed to raise the shared journals fallback alert for '{branchEnv.Options.BasePath}'", e);
        }
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
