using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Client.Smuggler;
using Raven.Server.Documents.PeriodicExport.Aws;
using Raven.Server.Documents.PeriodicExport.Azure;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.PeriodicExport
{
    public class PeriodicExportRunner : IDisposable
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(BundleLoader));

        private readonly DocumentDatabase _database;
        private readonly PeriodicExportConfiguration _configuration;
        private readonly PeriodicExportStatus _status;

        // This will be canceled once the configuration document will be changed
        private readonly CancellationTokenSource _cancellationToken;

        private Timer _incrementalExportTimer;
        private Timer _fullExportTimer;
        private TimeSpan _incrementalIntermediateInterval;
        private TimeSpan _fullExportIntermediateInterval;

        public readonly TimeSpan FullExportInterval;
        public readonly TimeSpan IncrementalInterval;

        private int? _exportLimit;

        //interval can be 2^32-2 milliseconds at most
        //this is the maximum interval acceptable in .Net's threading timer
        public readonly TimeSpan MaxTimerTimeout = TimeSpan.FromMilliseconds(Math.Pow(2, 32) - 2);

        /* TODO: How should we set this value, in the configuration document? If so, how do we encrypt them? */
        private string _awsAccessKey, _awsSecretKey;
        private string _azureStorageAccount, _azureStorageKey;

        private Task _runningTask;

        private PeriodicExportRunner(DocumentDatabase database, PeriodicExportConfiguration configuration, PeriodicExportStatus status)
        {
            _database = database;
            _configuration = configuration;
            _status = status;

            _cancellationToken = new CancellationTokenSource();

            if (configuration.IntervalMilliseconds.HasValue && configuration.IntervalMilliseconds.Value > 0)
            {
                _incrementalIntermediateInterval = IncrementalInterval = TimeSpan.FromMilliseconds(configuration.IntervalMilliseconds.Value);
                Log.Info($"Incremental periodic export started, will export every {IncrementalInterval.TotalMinutes} minutes");

                if (IsValidTimespanForTimer(IncrementalInterval))
                {
                    var lastExportAt = _status.LastExportAtTicks.HasValue ? new DateTime(_status.LastExportAtTicks.Value, DateTimeKind.Utc) : DateTime.MinValue;
                    var timeSinceLastExport = SystemTime.UtcNow - lastExportAt ;
                    var nextExport = timeSinceLastExport >= IncrementalInterval ? TimeSpan.Zero : IncrementalInterval - timeSinceLastExport;

                    _incrementalExportTimer = new Timer(TimerCallback, false, nextExport, IncrementalInterval);
                }
                else
                {
                    _incrementalExportTimer = new Timer(LongPeriodTimerCallback, false, MaxTimerTimeout, Timeout.InfiniteTimeSpan);
                }
            }
            else
            {
                Log.Warn("Incremental periodic export interval is set to zero or less, incremental periodic export is now disabled");
            }

            if (configuration.FullExportIntervalMilliseconds.HasValue && configuration.FullExportIntervalMilliseconds.Value > 0)
            {
                _fullExportIntermediateInterval = FullExportInterval = TimeSpan.FromMilliseconds(configuration.FullExportIntervalMilliseconds.Value);
                Log.Info("Full periodic export started, will export every" + FullExportInterval.TotalMinutes + "minutes");

                if (IsValidTimespanForTimer(FullExportInterval))
                {
                    var lastFullExportAt = _status.LastFullExportAtTicks.HasValue ? new DateTime(_status.LastFullExportAtTicks.Value, DateTimeKind.Utc) : DateTime.MinValue;
                    var timeSinceLastExport = SystemTime.UtcNow - lastFullExportAt;
                    var nextExport = timeSinceLastExport >= FullExportInterval ? TimeSpan.Zero : FullExportInterval - timeSinceLastExport;

                    _fullExportTimer = new Timer(TimerCallback, true, nextExport, FullExportInterval);
                }
                else
                {
                    _fullExportTimer = new Timer(LongPeriodTimerCallback, true, MaxTimerTimeout, Timeout.InfiniteTimeSpan);
                }
            }
            else
            {
                Log.Warn("Full periodic export interval is set to zero or less, full periodic export is now disabled");
            }
        }

        private void LongPeriodTimerCallback(object fullExport)
        {
            if (_database.DatabaseShutdown.IsCancellationRequested)
                return;

            lock (this)
            {
                if ((bool)fullExport)
                {
                    _fullExportTimer?.Dispose();
                    _fullExportTimer = ScheduleNextLongTimer(true);
                }
                else
                {
                    _incrementalExportTimer?.Dispose();
                    _incrementalExportTimer = ScheduleNextLongTimer(false);
                }
            }
        }

        private Timer ScheduleNextLongTimer(bool isFullbackup)
        {
            var intermediateTimespan = isFullbackup ? _fullExportIntermediateInterval : _incrementalIntermediateInterval;
            var remainingInterval = intermediateTimespan - MaxTimerTimeout;
            var shouldExecuteTimer = remainingInterval.TotalMilliseconds <= 0;
            if (shouldExecuteTimer)
            {
                TimerCallback(isFullbackup);
            }

            if (isFullbackup)
                _fullExportIntermediateInterval = shouldExecuteTimer ? FullExportInterval : remainingInterval;
            else
                _incrementalIntermediateInterval = shouldExecuteTimer ? IncrementalInterval : remainingInterval;

            return new Timer(LongPeriodTimerCallback, isFullbackup, shouldExecuteTimer ? MaxTimerTimeout : remainingInterval, Timeout.InfiniteTimeSpan);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsValidTimespanForTimer(TimeSpan timespan)
        {
            return timespan < MaxTimerTimeout;
        }

        private void TimerCallback(object fullExport)
        {
            if (_runningTask != null || _database.DatabaseShutdown.IsCancellationRequested)
                return;

            // we have shared lock for both incremental and full backup.
            lock (this)
            {
                if (_runningTask != null)
                    return;
                _runningTask = Task.Run(async () =>
                {
                    await RunPeriodicExport((bool)fullExport);
                });
                _runningTask.ContinueWith(task =>
                {
                    _runningTask = null;
                });
            }
        }

        private async Task RunPeriodicExport(bool fullExport)
        {
            if (_cancellationToken.IsCancellationRequested)
                return;

            if (Log.IsDebugEnabled)
                Log.Debug($"Exporting a {(fullExport ? "full" : "incremental")} export");

            try
            {
                DocumentsOperationContext context;
                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                {
                    var sp = Stopwatch.StartNew();
                    using (var tx = context.OpenReadTransaction())
                    {
                        if (fullExport == false)
                        {
                            var currentLastEtag = DocumentsStorage.ReadLastEtag(tx.InnerTransaction);
                            // No-op if nothing has changed
                            if (currentLastEtag == _status.LastDocsEtag)
                                return;
                        }

                        var exportDirectory = _configuration.LocalFolderName ?? Path.Combine(_database.Configuration.Core.DataDirectory, "PeriodicExport-Temp");
                        if (Directory.Exists(exportDirectory) == false)
                            Directory.CreateDirectory(exportDirectory);

                        var dataExporter = new DatabaseDataExporter(_database)
                        {
                            Limit = _exportLimit,
                        };

                        string exportFilePath;
                        var now = SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture);
                        string fileName;
                        if (fullExport)
                        {
                            // create filename for full export
                            fileName = $"{now}.ravendb-full-export";
                            exportFilePath = Path.Combine(exportDirectory, fileName);
                            if (File.Exists(exportFilePath))
                            {
                                var counter = 1;
                                while (true)
                                {
                                    fileName = $"{now} - {counter}.ravendb-full-export";
                                    exportFilePath = Path.Combine(exportDirectory, fileName);

                                    if (File.Exists(exportFilePath) == false)
                                        break;
                                    counter++;
                                }
                            }
                        }
                        else
                        {
                            // create filename for incremental export
                            fileName = $"{now}-0.ravendb-incremental-export";
                            exportFilePath = Path.Combine(exportDirectory, fileName);
                            if (File.Exists(exportFilePath))
                            {
                                var counter = 1;
                                while (true)
                                {
                                    fileName = $"{now}-{counter}.ravendb-incremental-export";
                                    exportFilePath = Path.Combine(exportDirectory, fileName);

                                    if (File.Exists(exportFilePath) == false)
                                        break;
                                    counter++;
                                }
                            }

                            dataExporter.StartDocsEtag = _status.LastDocsEtag;
                            if (dataExporter.StartDocsEtag == null)
                            {
                                IncrementalExport.ReadLastEtagsFromFile(exportDirectory, context, dataExporter);
                            }
                        }

                        var exportResult = dataExporter.Export(context, new DatabaseSmugglerFileDestination {FilePath = exportFilePath});

                        if (fullExport == false)
                        {
                            // No-op if nothing has changed
                            if (exportResult.LastDocsEtag == _status.LastDocsEtag)
                            {
                                Log.Info("Periodic export returned prematurely, nothing has changed since last export");
                                return;
                            }
                        }

                        try
                        {
                            await UploadToServer(exportFilePath, fileName, fullExport).ConfigureAwait(false);
                        }
                        finally
                        {
                            // if user did not specify local folder we delete temporary file.
                            if (string.IsNullOrEmpty(_configuration.LocalFolderName))
                            {
                                IOExtensions.DeleteFile(exportFilePath);
                            }
                        }

                        _status.LastDocsEtag = exportResult.LastDocsEtag;
                        if (fullExport)
                            _status.LastFullExportAtTicks = SystemTime.UtcNow.Ticks;
                        else
                            _status.LastExportAtTicks = SystemTime.UtcNow.Ticks;

                        WriteStatus();
                    }
                    if (Log.IsDebugEnabled)
                        Log.Debug($"Successfully exported {(fullExport ? "full" : "incremental")} export in {sp.ElapsedMilliseconds:#,#;;0} ms.");

                    _exportLimit = null;
                }
            }
            catch (Exception e)
            {
                _exportLimit = 100;
                Log.ErrorException("Error when performing periodic export", e);
                _database.AddAlert(new Alert
                {
                    IsError = true,
                    CreatedAt = SystemTime.UtcNow,
                    Message = e.Message,
                    Title = "Error in Periodic Export",
                    Exception = e.ToString(),
                    UniqueKey = "Periodic Export Error",
                });
            }
        }

        private void WriteStatus()
        {
            if (_cancellationToken.IsCancellationRequested)
                return;

            DocumentsOperationContext context;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var status = new DynamicJsonValue
                {
                    ["LastDocsEtag"] = _status.LastDocsEtag,
                    ["LastExportAtTicks"] = _status.LastExportAtTicks,
                    ["LastFullExportAtTicks"] = _status.LastFullExportAtTicks,
                };
                var readerObject = context.ReadObject(status, Constants.PeriodicExport.StatusDocumentKey, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                var putResult = _database.DocumentsStorage.Put(context, Constants.PeriodicExport.StatusDocumentKey, null, readerObject);
                tx.Commit();

                if (_status.LastDocsEtag + 1 == putResult.ETag) // the last etag is with just us
                    _status.LastDocsEtag = putResult.ETag; // so we can skip it for the next time
            }
        }

        private async Task UploadToServer(string exportPath, string fileName, bool isFullExport)
        {
            if (!string.IsNullOrWhiteSpace(_configuration.GlacierVaultName))
            {
                await UploadToGlacier(exportPath, fileName, isFullExport).ConfigureAwait(false);
            }
            else if (!string.IsNullOrWhiteSpace(_configuration.S3BucketName))
            {
                await UploadToS3(exportPath, fileName, isFullExport).ConfigureAwait(false);
            }
            else if (!string.IsNullOrWhiteSpace(_configuration.AzureStorageContainer))
            {
                await UploadToAzure(exportPath, fileName, isFullExport).ConfigureAwait(false);
            }
        }

        private async Task UploadToS3(string exportPath, string fileName, bool isFullExport)
        {
            if (_awsAccessKey == Constants.DataCouldNotBeDecrypted ||
                _awsSecretKey == Constants.DataCouldNotBeDecrypted)
            {
                throw new InvalidOperationException("Could not decrypt the AWS access settings, if you are running on IIS, make sure that load user profile is set to true.");
            }

            using (var client = new RavenAwsS3Client(_awsAccessKey, _awsSecretKey, _configuration.AwsRegionName ?? RavenAwsClient.DefaultRegion))
            using (var fileStream = File.OpenRead(exportPath))
            {
                var key = CombinePathAndKey(_configuration.S3RemoteFolderName, fileName);
                await client.PutObject(_configuration.S3BucketName, key, fileStream, new Dictionary<string, string>
                {
                    {"Description", GetArchiveDescription(isFullExport)}
                }, 60*60).ConfigureAwait(false);

                Log.Info(string.Format("Successfully uploaded export {0} to S3 bucket {1}, with key {2}", fileName, _configuration.S3BucketName, key));
            }
        }

        private async Task UploadToGlacier(string exportPath, string fileName, bool isFullExport)
        {
            if (_awsAccessKey == Constants.DataCouldNotBeDecrypted ||
                _awsSecretKey == Constants.DataCouldNotBeDecrypted)
            {
                throw new InvalidOperationException("Could not decrypt the AWS access settings, if you are running on IIS, make sure that load user profile is set to true.");
            }

            using (var client = new RavenAwsGlacierClient(_awsAccessKey, _awsSecretKey, _configuration.AwsRegionEndpoint ?? RavenAwsClient.DefaultRegion))
            using (var fileStream = File.OpenRead(exportPath))
            {
                var archiveId = await client.UploadArchive(_configuration.GlacierVaultName, fileStream, fileName, 60*60).ConfigureAwait(false);
                Log.Info($"Successfully uploaded export {fileName} to Glacier, archive ID: {archiveId}");
            }
        }

        private async Task UploadToAzure(string exportPath, string fileName, bool isFullExport)
        {
            if (_azureStorageAccount == Constants.DataCouldNotBeDecrypted ||
                _azureStorageKey == Constants.DataCouldNotBeDecrypted)
            {
                throw new InvalidOperationException("Could not decrypt the Azure access settings, if you are running on IIS, make sure that load user profile is set to true.");
            }

            using (var client = new RavenAzureClient(_azureStorageAccount, _azureStorageKey, _configuration.AzureStorageContainer))
            {
                await client.PutContainer().ConfigureAwait(false);
                using (var fileStream = File.OpenRead(exportPath))
                {
                    var key = CombinePathAndKey(_configuration.AzureRemoteFolderName, fileName);
                    await client.PutBlob(key, fileStream, new Dictionary<string, string>
                    {
                        {"Description", GetArchiveDescription(isFullExport)}
                    }).ConfigureAwait(false);

                    Log.Info($"Successfully uploaded export {fileName} to Azure container {_configuration.AzureStorageContainer}, with key {key}");
                }
            }
        }

        private string CombinePathAndKey(string path, string fileName)
        {
            return string.IsNullOrEmpty(path) == false ? path + "/" + fileName : fileName;
        }

        private string GetArchiveDescription(bool isFullExport)
        {
            return $"{(isFullExport ? "Full" : "Incremental")} periodic export for db {_database.Name} at {SystemTime.UtcNow}";
        }

        public void Dispose()
        {
            _cancellationToken.Cancel();
            _incrementalExportTimer?.Dispose();
            _fullExportTimer?.Dispose();
            var task = _runningTask;
            task?.Wait();
        }

        public static PeriodicExportRunner LoadConfigurations(DocumentDatabase database)
        {
            DocumentsOperationContext context;
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var configuration = database.DocumentsStorage.Get(context, Constants.PeriodicExport.ConfigurationDocumentKey);
                if (configuration == null)
                    return null;

                try
                {
                    var periodicExportConfiguration = JsonDeserialization.PeriodicExportConfiguration(configuration.Data);
                    if (periodicExportConfiguration.Active == false)
                    {
                        Log.Info("Periodic export is disabled.");
                        return null;
                    }

                    var status = database.DocumentsStorage.Get(context, Constants.PeriodicExport.StatusDocumentKey);
                    PeriodicExportStatus periodicExportStatus = null;
                    if (status != null)
                    {
                        try
                        {
                            periodicExportStatus = JsonDeserialization.PeriodicExportStatus(status.Data);
                        }
                        catch (Exception e)
                        {
                            if (Log.IsWarnEnabled)
                                Log.WarnException($"Unable to read the periodic export status as the status document {Constants.PeriodicExport.StatusDocumentKey} is not valid. We will start to export from scratch. Data: {configuration.Data}", e);
                        }
                    }

                    return new PeriodicExportRunner(database, periodicExportConfiguration, periodicExportStatus ?? new PeriodicExportStatus());
                }
                catch (Exception e)
                {
                    //TODO: Raise alert, or maybe handle this via a db load error that can be turned off with 
                    //TODO: a config
                    if (Log.IsWarnEnabled)
                        Log.WarnException($"Cannot enable periodic export as the configuration document {Constants.PeriodicExport.ConfigurationDocumentKey} is not valid: {configuration.Data}", e);
                    /*
                     Database.AddAlert(new Alert
                                    {
                                        AlertLevel = AlertLevel.Error,
                                        CreatedAt = SystemTime.UtcNow,
                                        Message = ex.Message,
                                        Title = "Could not read periodic export config",
                                        Exception = ex.ToString(),
                                        UniqueKey = "Periodic Export Config Error"
                                    });*/
                    return null;
                }
            }
        }
    }
}