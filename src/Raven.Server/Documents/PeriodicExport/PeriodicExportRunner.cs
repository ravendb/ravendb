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
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Client.Data;
using Raven.Client.Smuggler;
using Raven.Json.Linq;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

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

        private readonly Timer _incrementalExportTimer;
        private readonly TimeSpan _incrementalIntermediateInterval;
        private readonly Timer _fullExportTimer;
        private readonly TimeSpan _fullExportIntermediateInterval;

        private TimeSpan _fullExportInterval;
        private TimeSpan _incrementalInterval;

        private readonly object _locker = new object();
        private int? _exportLimit;

        //interval can be 2^32-2 milliseconds at most
        //this is the maximum interval acceptable in .Net's threading timer
        private readonly TimeSpan _maxTimerTimeout = TimeSpan.FromMilliseconds(Math.Pow(2, 32) - 2);

        private PeriodicExportRunner(DocumentDatabase database, PeriodicExportConfiguration configuration, PeriodicExportStatus status)
        {
            _database = database;
            _configuration = configuration;
            _status = status;

            _cancellationToken = new CancellationTokenSource();

            if (configuration.IntervalMilliseconds.HasValue && configuration.IntervalMilliseconds.Value > 0)
            {
                _incrementalIntermediateInterval = _incrementalInterval = TimeSpan.FromMilliseconds(configuration.IntervalMilliseconds.Value);
                Log.Info($"Incremental periodic export started, will export every {_incrementalInterval.TotalMinutes} minutes");

                if (IsValidTimespanForTimer(_incrementalInterval))
                {
                    var timeSinceLastExport = SystemTime.UtcNow - _status.LastExportAt;
                    var nextExport = timeSinceLastExport >= _incrementalInterval ? TimeSpan.Zero : _incrementalInterval - timeSinceLastExport;

                    _incrementalExportTimer = new Timer(TimerCallback, false, nextExport, _incrementalInterval);
                }
                else
                {
                    _incrementalExportTimer = new Timer(LongPeriodTimerCallback, false, _maxTimerTimeout, Timeout.InfiniteTimeSpan);
                }
            }
            else
            {
                Log.Warn("Incremental periodic export interval is set to zero or less, incremental periodic export is now disabled");
            }

            if (configuration.FullExportIntervalMilliseconds.HasValue && configuration.FullExportIntervalMilliseconds.Value > 0)
            {
                _fullExportIntermediateInterval = _fullExportInterval = TimeSpan.FromMilliseconds(configuration.FullExportIntervalMilliseconds.Value);
                Log.Info("Full periodic export started, will export every" + _fullExportInterval.TotalMinutes + "minutes");

                if (IsValidTimespanForTimer(_fullExportInterval))
                {
                    var timeSinceLastExport = SystemTime.UtcNow - _status.LastFullExportAt;
                    var nextExport = timeSinceLastExport >= _fullExportInterval ? TimeSpan.Zero : _fullExportInterval - timeSinceLastExport;

                    _fullExportTimer = new Timer(TimerCallback, true, nextExport, _fullExportInterval);
                }
                else
                {
                    _fullExportTimer = new Timer(LongPeriodTimerCallback, true, _maxTimerTimeout, Timeout.InfiniteTimeSpan);
                }
            }
            else
            {
                Log.Warn("Full periodic export interval is set to zero or less, full periodic export is now disabled");
            }
        }

        private void LongPeriodTimerCallback(object state)
        {
           /* lock (this)
            {
                if (fullExport)
                {
                    ReleaseTimerIfNeeded(_fullExportTimer);
                    _fullExportTimer = RescheduleLongTimer(true);
                }
                else
                {
                    ReleaseTimerIfNeeded(_incrementalExportTimer);
                    _incrementalExportTimer = RescheduleLongTimer(false);
                }
            }*/
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsValidTimespanForTimer(TimeSpan timespan)
        {
            return timespan < _maxTimerTimeout;
        }

        private void TimerCallback(object _)
        {
            if (_database.DatabaseShutdown.IsCancellationRequested)
                return;

            if (Monitor.TryEnter(_locker) == false)
                return;

            try
            {
                 RunPeriodicExport().Wait();
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
            finally
            {
                Monitor.Exit(_locker);
            }
        }

        private async Task RunPeriodicExport(bool fullExport)
        {
            if (_cancellationToken.IsCancellationRequested)
                return;

            if (Log.IsDebugEnabled)
                Log.Debug($"Exporting a {(fullExport ? "full" : "incremental")} export");

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

                    if (fullExport)
                    {
                        // create filename for full dump
                        var now = SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture);
                        var exportFilePath = Path.Combine(exportDirectory, $"{now}.ravendb-full-export");
                        if (File.Exists(exportFilePath))
                        {
                            var counter = 1;
                            while (true)
                            {
                                exportFilePath = Path.Combine(exportDirectory, $"{now} - {counter}.ravendb-full-export");

                                if (File.Exists(exportFilePath) == false)
                                    break;

                                counter++;
                            }
                        }
                    }

                    var dataExporter = new DatabaseDataExporter(_database)
                    {
                        Limit = _exportLimit,
                    };

                    if (fullExport == false)
                    {
                        dataExporter.StartDocsEtag = _status.LastDocsEtag;
                        dataExporter.Incremental = true;
                    }
                    var exportResult = await dataExporter.Export(new DatabaseSmugglerFileDestination { FilePath = exportFilePath }).ConfigureAwait(false);

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
                        UploadToServer(exportResult.FilePath, _configuration, fullExport);
                    }
                    finally
                    {
                        // if user did not specify local folder we delete temporary file.
                        if (string.IsNullOrEmpty(_configuration.LocalFolderName))
                        {
                            IOExtensions.DeleteFile(exportResult.FilePath);
                        }
                    }

                    _status.LastDocsEtag = exportResult.LastDocsEtag;
                    if (fullExport)
                        _status.LastFullExportAt = SystemTime.UtcNow;
                    else
                        _status.LastExportAt = SystemTime.UtcNow;

                    WriteStatus();
                }
                if (Log.IsDebugEnabled)
                    Log.Debug($"Successfully exported {(fullExport ? "full" : "incremental")} export in {sp.ElapsedMilliseconds:#,#;;0} ms.");

                _exportLimit = null;
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
                    ["LastExportAt"] = _status.LastExportAt,
                    ["LastFullExportAt"] = _status.LastFullExportAt,
                };
                var readerObject = context.ReadObject(status, Constants.PeriodicExport.StatusDocumentKey, BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                var putResult = _database.DocumentsStorage.Put(context, Constants.PeriodicExport.StatusDocumentKey, null, readerObject);
                tx.Commit();

                if (_status.LastDocsEtag + 1 == putResult.ETag) // the last etag is with just us
                    _status.LastDocsEtag = putResult.ETag; // so we can skip it for the next time
            }
        }

        private void UploadToServer(string exportPath, PeriodicExportSetup localExportConfigs, bool isFullExport)
        {
            if (!string.IsNullOrWhiteSpace(localExportConfigs.GlacierVaultName))
            {
                UploadToGlacier(exportPath, localExportConfigs, isFullExport);
            }
            else if (!string.IsNullOrWhiteSpace(localExportConfigs.S3BucketName))
            {
                UploadToS3(exportPath, localExportConfigs, isFullExport);
            }
            else if (!string.IsNullOrWhiteSpace(localExportConfigs.AzureStorageContainer))
            {
                UploadToAzure(exportPath, localExportConfigs, isFullExport);
            }
        }

        private void UploadToS3(string exportPath, PeriodicExportSetup localExportConfigs, bool isFullExport)
        {
            if (awsAccessKey == Constants.DataCouldNotBeDecrypted ||
                awsSecretKey == Constants.DataCouldNotBeDecrypted)
            {
                throw new InvalidOperationException("Could not decrypt the AWS access settings, if you are running on IIS, make sure that load user profile is set to true.");
            }
            using (var client = new RavenAwsS3Client(awsAccessKey, awsSecretKey, localExportConfigs.AwsRegionEndpoint ?? RavenAwsClient.DefaultRegion))
            using (var fileStream = File.OpenRead(exportPath))
            {
                var key = Path.GetFileName(exportPath);
                client.PutObject(localExportConfigs.S3BucketName, CombinePathAndKey(localExportConfigs.S3RemoteFolderName, key), fileStream, new Dictionary<string, string>
                                                                                   {
                                                                                       { "Description", GetArchiveDescription(isFullExport) }
                                                                                   }, 60 * 60);

                Log.Info(string.Format("Successfully uploaded export {0} to S3 bucket {1}, with key {2}",
                                              Path.GetFileName(exportPath), localExportConfigs.S3BucketName, key));
            }
        }

        private void UploadToGlacier(string exportPath, PeriodicExportSetup localExportConfigs, bool isFullExport)
        {
            if (awsAccessKey == Constants.DataCouldNotBeDecrypted ||
                awsSecretKey == Constants.DataCouldNotBeDecrypted)
            {
                throw new InvalidOperationException("Could not decrypt the AWS access settings, if you are running on IIS, make sure that load user profile is set to true.");
            }
            using (var client = new RavenAwsGlacierClient(awsAccessKey, awsSecretKey, localExportConfigs.AwsRegionEndpoint ?? RavenAwsClient.DefaultRegion))
            using (var fileStream = File.OpenRead(exportPath))
            {
                var key = Path.GetFileName(exportPath);
                var archiveId = client.UploadArchive(localExportConfigs.GlacierVaultName, fileStream, key, 60 * 60);
                Log.Info(string.Format("Successfully uploaded export {0} to Glacier, archive ID: {1}", Path.GetFileName(exportPath), archiveId));
            }
        }

        private void UploadToAzure(string exportPath, PeriodicExportSetup localExportConfigs, bool isFullExport)
        {
            if (azureStorageAccount == Constants.DataCouldNotBeDecrypted ||
                azureStorageKey == Constants.DataCouldNotBeDecrypted)
            {
                throw new InvalidOperationException("Could not decrypt the Azure access settings, if you are running on IIS, make sure that load user profile is set to true.");
            }

            using (var client = new RavenAzureClient(azureStorageAccount, azureStorageKey))
            {
                client.PutContainer(localExportConfigs.AzureStorageContainer);
                using (var fileStream = File.OpenRead(exportPath))
                {
                    var key = Path.GetFileName(exportPath);
                    client.PutBlob(localExportConfigs.AzureStorageContainer, CombinePathAndKey(localExportConfigs.AzureRemoteFolderName, key), fileStream, new Dictionary<string, string>
                                                                                              {
                                                                                                  { "Description", GetArchiveDescription(isFullExport) }
                                                                                              });

                    Log.Info(string.Format(
                        "Successfully uploaded export {0} to Azure container {1}, with key {2}",
                        Path.GetFileName(exportPath),
                        localExportConfigs.AzureStorageContainer,
                        key));
                }
            }
        }

        private string CombinePathAndKey(string path, string fileName)
        {
            return string.IsNullOrEmpty(path) == false ? path + "/" + fileName : fileName;
        }

        private string GetArchiveDescription(bool isFullExport)
        {
            return (isFullExport ? "Full" : "Incremental") + "periodic export for db " + (Database.Name ?? Constants.SystemDatabase) + " at " + SystemTime.UtcNow;
        }

        public void Dispose()
        {
            _cancellationToken.Cancel();
            _incrementalExportTimer?.Dispose();
            _fullExportTimer?.Dispose();
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