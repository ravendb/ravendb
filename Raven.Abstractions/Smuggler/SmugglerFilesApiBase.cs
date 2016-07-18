#if !DNXCORE50
using System.Text.RegularExpressions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Abstractions.Smuggler
{
    public class SmugglerFilesApiBase : ISmugglerApi<FilesConnectionStringOptions, SmugglerFilesOptions, ExportFilesResult>
    {
        private const string MetadataEntry = ".metadata";
        private const string ConfigurationsEntry = ".configurations";

        private readonly Regex internalConfigs = new Regex("^(sync|deleteOp|raven\\/synchronization\\/sources|conflicted|renameOp)", RegexOptions.IgnoreCase);

        private class FileContainer
        {
            public string Key;
            public RavenJObject Metadata;

            [JsonIgnore]
            public bool IsTombstone
            {
                get
                {
                    if (Metadata.ContainsKey(Constants.RavenDeleteMarker))
                        return Metadata[Constants.RavenDeleteMarker].Value<bool>();

                    return false;
                }
            }
        }

        private class ConfigContainer
        {
            public string Name;
            public RavenJObject Value;
        }

        public SmugglerFilesOptions Options { get; private set; }

        public ISmugglerFilesOperations Operations { get; protected set; }

        private const string IncrementalExportStateFile = "IncrementalExport.state.json";

        protected SmugglerFilesApiBase(SmugglerFilesOptions options)
        {
            this.Options = options;
        }

        public virtual async Task<ExportFilesResult> ExportData(SmugglerExportOptions<FilesConnectionStringOptions> exportOptions)
        {
            Operations.Configure(Options);
            Operations.Initialize(Options);

            var result = new ExportFilesResult
            {
                FilePath = exportOptions.ToFile,
                LastFileEtag = Options.StartFilesEtag,
                LastDeletedFileEtag = Options.StartFilesDeletionEtag,
            };

            if (result.FilePath != null)
            {
                result.FilePath = Path.GetFullPath(result.FilePath);
            }

            if (Options.Incremental)
            {
                if (Directory.Exists(result.FilePath) == false)
                {
                    if (File.Exists(result.FilePath))
                        result.FilePath = Path.GetDirectoryName(result.FilePath) ?? result.FilePath;
                    else
                        Directory.CreateDirectory(result.FilePath);
                }

                if (Options.StartFilesEtag == Etag.Empty)
                {
                    ReadLastEtagsFromFile(result);
                }

                result.FilePath = Path.Combine(result.FilePath, SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm-0", CultureInfo.InvariantCulture) + ".ravenfs-incremental-dump");
                if (File.Exists(result.FilePath))
                {
                    var counter = 1;
                    while (true)
                    {
                        result.FilePath = Path.Combine(Path.GetDirectoryName(result.FilePath), SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture) + "-" + counter + ".ravenfs-incremental-dump");

                        if (File.Exists(result.FilePath) == false)
                            break;

                        counter++;
                    }
                }
            }

            SmugglerExportException lastException = null;

            bool ownedStream = exportOptions.ToStream == null;
            var stream = exportOptions.ToStream ?? File.Create(result.FilePath);

            try
            {
                await DetectServerSupportedFeatures(exportOptions.From).ConfigureAwait(false);
            }
            catch (WebException e)
            {
                throw new SmugglerExportException("Failed to query server for supported features. Reason : " + e.Message)
                {
                    LastEtag = Etag.Empty,
                    File = result.FilePath
                };
            }

            try
            {
                // used to synchronize max returned values for put/delete operations
                var maxEtags = Operations.FetchCurrentMaxEtags();

                try
                {
                    // We use PositionWrapperStream due to:
                    // http://connect.microsoft.com/VisualStudio/feedbackdetail/view/816411/ziparchive-shouldnt-read-the-position-of-non-seekable-streams
                    using (var positionStream = new PositionWrapperStream(stream, leaveOpen: true))
                    using (var archive = new ZipArchive(positionStream, ZipArchiveMode.Create, leaveOpen: true))
                    {

                        await ExportFiles(archive, result.LastFileEtag, maxEtags.LastFileEtag).ConfigureAwait(false);
                        await ExportConfigurations(archive).ConfigureAwait(false);
                    }
                }
                catch (SmugglerExportException ex)
                {
                    result.LastFileEtag = ex.LastEtag;
                    ex.File = result.FilePath;
                    lastException = ex;
                }

                if (Options.Incremental)
                {
                    WriteLastEtagsToFile(result, Path.GetDirectoryName(result.FilePath));
                }

                if (lastException != null)
                    throw lastException;

                return result;
            }
            finally
            {
                if (ownedStream && stream != null)
                    stream.Dispose();
            }
        }

        private async Task<Etag> ExportFiles(ZipArchive archive, Etag lastEtag, Etag maxEtag)
        {
            var totalCount = 0;
            var lastReport = SystemTime.UtcNow;
            var reportInterval = TimeSpan.FromSeconds(2);
            Operations.ShowProgress("Exporting Files");

            var metadataList = new List<FileContainer>();

            Exception exceptionHappened = null;

            using (var cts = new CancellationTokenSource())
            {
                var fileHeaders = new BlockingCollection<FileHeader>();
                var getFilesTask = Task.Run(async () => await GetFilesTask(lastEtag, maxEtag, cts, fileHeaders).ConfigureAwait(false), cts.Token);

                try
                {
                    while (true)
                    {
                        FileHeader fileHeader = null;
                        try
                        {
                            fileHeader = fileHeaders.Take(cts.Token);
                        }
                        catch (InvalidOperationException) // CompleteAdding Called
                        {
                            Operations.ShowProgress("Files List Retrieval Completed");
                            break;
                        }

                        cts.Token.ThrowIfCancellationRequested();

                        // Write the metadata (which includes the stream size and file container name)
                        var fileContainer = new FileContainer
                        {
                            Key = Path.Combine(fileHeader.Directory.TrimStart('/'), fileHeader.Name),
                            Metadata = fileHeader.Metadata,
                        };

                        ZipArchiveEntry fileToStore = archive.CreateEntry(fileContainer.Key);

                        using (var fileStream = await Operations.DownloadFile(fileHeader).ConfigureAwait(false))
                        using (var zipStream = fileToStore.Open())
                        {
                            await fileStream.CopyToAsync(zipStream).ConfigureAwait(false);
                        }

                        metadataList.Add(fileContainer);

                        totalCount++;
                        if (totalCount%30 == 0 || SystemTime.UtcNow - lastReport > reportInterval)
                        {
                            Operations.ShowProgress("Exported {0} files. ", totalCount);
                            lastReport = SystemTime.UtcNow;
                        }
                    }
                }
                catch (Exception e)
                {
                    Operations.ShowProgress("Got Exception during smuggler export. Exception: {0}. ", e);
                    Operations.ShowProgress("Done with reading files, total: {0}, lastEtag: {1}", totalCount, lastEtag);

                    cts.Cancel();

                    exceptionHappened = new SmugglerExportException(e.Message, e)
                    {
                        LastEtag = lastEtag,
                    };
                }

                try
                {
                    getFilesTask.Wait(CancellationToken.None);
                }
                catch (OperationCanceledException)
                {
                    // we are fine with this
                }
                catch (Exception e)
                {
                    Operations.ShowProgress("Got Exception during smuggler export. Exception: {0}. ", e.Message);
                    exceptionHappened = new SmugglerExportException(e.Message, e)
                    {
                        LastEtag = lastEtag,
                    };
                }
            }


            var metadataEntry = archive.CreateEntry(MetadataEntry);
            using (var metadataStream = metadataEntry.Open())
            using (var writer = new StreamWriter(metadataStream))
            {
                foreach (var item in metadataList)
                    writer.WriteLine(RavenJObject.FromObject(item));
            }

            if (exceptionHappened != null)
                throw exceptionHappened;

            Operations.ShowProgress("Done with reading documents, total: {0}, lastEtag: {1}", totalCount, lastEtag);
            return lastEtag;
        }

        private async Task GetFilesTask(Etag lastEtag, Etag maxEtag, CancellationTokenSource cts, BlockingCollection<FileHeader> fileHeaders)
        {
            while (true)
            {
                try
                {
                    if (cts.IsCancellationRequested)
                        break;

                    using (var files = await Operations.GetFiles(lastEtag, Options.BatchSize).ConfigureAwait(false))
                    {
                        var hasDocs = false;
                        while (await files.MoveNextAsync().ConfigureAwait(false))
                        {
                            if (cts.IsCancellationRequested)
                                break;

                            var file = files.Current;

                            hasDocs = true;

                            if (file.IsTombstone)
                            {
                                lastEtag = file.Etag;
                                continue;
                            }

                            var tempLastEtag = file.Etag;
                            if (maxEtag != null && tempLastEtag.CompareTo(maxEtag) > 0)
                                break;

                            fileHeaders.Add(files.Current);

                            lastEtag = tempLastEtag;
                        }

                        if (hasDocs == false)
                        {
                            fileHeaders.CompleteAdding();
                            break;
                        }
                    }
                }
                catch (Exception)
                {
                    cts.Cancel();
                    fileHeaders.CompleteAdding();
                    throw;
                }
            }
        }

        private async Task ExportConfigurations(ZipArchive archive)
        {
            var totalCount = 0;
            var lastReport = SystemTime.UtcNow;
            var reportInterval = TimeSpan.FromSeconds(2);

            Operations.ShowProgress("Exporting Configurations");

            var configurations = archive.CreateEntry(ConfigurationsEntry);

            using (var zipStream = configurations.Open())
            using (var streamWriter = new StreamWriter(zipStream))
            {
                while (true)
                {
                    bool hasConfigs = false;

                    foreach (var config in await Operations.GetConfigurations(totalCount, Options.BatchSize).ConfigureAwait(false))
                    {
                        if (internalConfigs.IsMatch(config.Key))
                            continue;

                        hasConfigs = true;

                        streamWriter.WriteLine(RavenJObject.FromObject(new ConfigContainer()
                        {
                            Name = config.Key,
                            Value = EnsureValidExportConfig(config)
                        }));

                        totalCount++;

                        if (totalCount % 100 == 0 || SystemTime.UtcNow - lastReport > reportInterval)
                        {
                            Operations.ShowProgress("Exported {0} configurations. ", totalCount);
                            lastReport = SystemTime.UtcNow;
                        }
                    }

                    if(hasConfigs == false)
                        break;
                }
            }

            Operations.ShowProgress("Done with exporting configurations");
            
        }

        private static RavenJObject EnsureValidExportConfig(KeyValuePair<string, RavenJObject> config)
        {
            if (string.Equals(config.Key, SynchronizationConstants.RavenSynchronizationDestinations, StringComparison.OrdinalIgnoreCase))
            {
                var destinationsConfig = config.Value.JsonDeserialization<SynchronizationDestinationsConfig>();

                foreach (var destination in destinationsConfig.Destinations)
                {
                    destination.Enabled = false;
                }

                return RavenJObject.FromObject(destinationsConfig);
            }

            return config.Value;
        }

        private async Task DetectServerSupportedFeatures(FilesConnectionStringOptions filesConnectionStringOptions)
        {
            var serverVersion = await this.Operations.GetVersion(filesConnectionStringOptions).ConfigureAwait(false);
            if (string.IsNullOrEmpty(serverVersion))
                throw new SmugglerExportException("Server version is not available.");


            var customAttributes = typeof(SmugglerDatabaseApiBase).Assembly.GetCustomAttributes(false);
            dynamic versionAtt = customAttributes.Single(x => x.GetType().Name == "RavenVersionAttribute");
            var intServerVersion = int.Parse(versionAtt.Version.Replace(".", ""));

            if (intServerVersion < 30)
                throw new SmugglerExportException(string.Format("File Systems are not available on Server version: {0}. Smuggler version: {1}.", serverVersion, versionAtt.Version));
        }

        private static void ReadLastEtagsFromFile(ExportFilesResult result)
        {
            var log = LogManager.GetCurrentClassLogger();

            var etagFileLocation = Path.Combine(result.FilePath, IncrementalExportStateFile);
            if (!File.Exists(etagFileLocation))
                return;

            using (var streamReader = new StreamReader(new FileStream(etagFileLocation, FileMode.Open)))
            using (var jsonReader = new JsonTextReader(streamReader))
            {
                RavenJObject ravenJObject;
                try
                {
                    ravenJObject = RavenJObject.Load(jsonReader);
                }
                catch (Exception e)
                {
                    log.WarnException("Could not parse etag document from file : " + etagFileLocation + ", ignoring, will start from scratch", e);
                    return;
                }
                result.LastFileEtag = Etag.Parse(ravenJObject.Value<string>("LastFileEtag"));
                result.LastDeletedFileEtag = Etag.Parse(ravenJObject.Value<string>("LastDeletedFileEtag") ?? Etag.Empty.ToString());
            }
        }

        public static void WriteLastEtagsToFile(ExportFilesResult result, string backupPath)
        {
            var etagFileLocation = Path.Combine(backupPath, IncrementalExportStateFile);
            using (var streamWriter = new StreamWriter(File.Create(etagFileLocation)))
            {
                new RavenJObject
                    {
                        {"LastFileEtag", result.LastFileEtag.ToString()},
                        {"LastDeletedFileEtag", result.LastDeletedFileEtag.ToString()},
                    }.WriteTo(new JsonTextWriter(streamWriter));
            }
        }

        public virtual async Task ImportData(SmugglerImportOptions<FilesConnectionStringOptions> importOptions)
        {
            Operations.ShowProgress("Importing filesystem");

            if (Options.Incremental == false)
            {
               Stream stream = importOptions.FromStream;
                bool ownStream = false;
                try
                {
                    if (stream == null)
                    {
                        stream = File.OpenRead(importOptions.FromFile);
                        ownStream = true;
                    }
                    await ImportData(importOptions, stream).ConfigureAwait(false);
                }
                finally
                {
                    if (stream != null && ownStream)
                        stream.Dispose();
                }
                return;
            }

            var directory = new DirectoryInfo(importOptions.FromFile);
            if (!directory.Exists)
                throw new InvalidOperationException("The directory does not exists.");

            var files = Directory.GetFiles(directory.FullName)
                            .Where(file => Path.GetExtension(file).Equals(".ravenfs-incremental-dump", StringComparison.CurrentCultureIgnoreCase))
                            .OrderBy(x => File.GetLastWriteTimeUtc(x) )
                            .ToArray();

            if (files.Length == 0)
                return;

            foreach (string filename in files)
            {
                using (var fileStream = File.OpenRead(filename))
                {
                    await ImportData(importOptions, fileStream).ConfigureAwait(false);
                }
            }
        }

        private async Task ImportData(SmugglerImportOptions<FilesConnectionStringOptions> importOptions, Stream stream)
        {
            Operations.Configure(Options);
            Operations.Initialize(Options);

            await DetectServerSupportedFeatures(importOptions.To).ConfigureAwait(false);

            var sw = Stopwatch.StartNew();

            var serializer = JsonExtensions.CreateDefaultJsonSerializer();

            // We open the zip file. 
            using (var archive = new ZipArchive(stream, ZipArchiveMode.Read))
            {            
                var filesLookup = archive.Entries.ToDictionary( x => x.FullName );

                var configurationsCount = 0;

                ZipArchiveEntry configurationsEntry;
                if (filesLookup.TryGetValue(ConfigurationsEntry, out configurationsEntry)) // older exports can not have it
                {
                    using (var streamReader = new StreamReader(configurationsEntry.Open()))
                    {
                        foreach (var json in streamReader.EnumerateJsonObjects())
                        {
                            var config = serializer.Deserialize<ConfigContainer>(new StringReader(json));

                            if (Options.StripReplicationInformation)
                            {
                                if (config.Name.Equals(SynchronizationConstants.RavenSynchronizationVersionHiLo, StringComparison.OrdinalIgnoreCase))
                                    continue;
                            }

                            await Operations.PutConfig(config.Name, config.Value).ConfigureAwait(false);

                            configurationsCount++;

                            if (configurationsCount%100 == 0)
                            {
                                Operations.ShowProgress("Read {0:#,#;;0} configurations", configurationsCount);
                            }
                        }
                    }
                }

                var filesCount = 0;

                var metadataEntry = filesLookup[MetadataEntry];
                using ( var streamReader = new StreamReader(metadataEntry.Open()) )
                {
                    foreach (var json in streamReader.EnumerateJsonObjects())
                    {
                        // For each entry in the metadata file.                        
                        var container = serializer.Deserialize<FileContainer>(new StringReader(json));

                        var header = new FileHeader(container.Key, container.Metadata);
                        if (header.IsTombstone)
                            continue;

                        var entry = filesLookup[container.Key];
                        using (var dataStream = entry.Open())
                        {
                            if (Options.StripReplicationInformation) 
                                container.Metadata = Operations.StripReplicationInformationFromMetadata(container.Metadata);

                            if(Options.ShouldDisableVersioningBundle)
                                container.Metadata = Operations.DisableVersioning(container.Metadata);
                                                        
                            await Operations.PutFile(header, dataStream, entry.Length).ConfigureAwait(false);
                        }

                        Options.CancelToken.Token.ThrowIfCancellationRequested();
                        filesCount++;

                        if (filesCount%100 == 0)
                        {
                            Operations.ShowProgress("Read {0:#,#;;0} files", filesCount);
                        }
                    }

                    Options.CancelToken.Token.ThrowIfCancellationRequested();
                }
            }

            sw.Stop();
        }

        public virtual async Task Between(SmugglerBetweenOptions<FilesConnectionStringOptions> betweenOptions)
        {
            Operations.Configure(Options);
            Operations.Initialize(Options);

            try
            {
                await DetectServerSupportedFeatures(betweenOptions.From).ConfigureAwait(false);
                await DetectServerSupportedFeatures(betweenOptions.To).ConfigureAwait(false);
            }
            catch (WebException e)
            {
                throw new SmugglerExportException("Failed to query server for supported features. Reason : " + e.Message)
                {
                    LastEtag = Etag.Empty,
                };
            }

            if (string.IsNullOrWhiteSpace(betweenOptions.IncrementalKey))
            {
                betweenOptions.IncrementalKey = Operations.CreateIncrementalKey();
            }

            var incremental = new ExportFilesDestinationKey();
            if (this.Options.Incremental)
            {
                var smugglerExportIncremental = await Operations.GetIncrementalExportKey().ConfigureAwait(false); // importStore.AsyncFilesCommands.Configuration.GetKeyAsync<SmugglerExportIncremental>(SmugglerExportIncremental.RavenDocumentKey);

                ExportFilesDestinationKey value;
                if (smugglerExportIncremental.Destinations.TryGetValue(betweenOptions.IncrementalKey, out value))
                {
                    incremental = value;
                }

                this.Options.StartFilesEtag = incremental.LastEtag ?? Etag.Empty;
            }

            var result = new ExportFilesResult
            {
                LastFileEtag = Options.StartFilesEtag,
            };

            // used to synchronize max returned values for put/delete operations
            var maxEtags = Operations.FetchCurrentMaxEtags();

            incremental.LastEtag = await CopyBetweenStores(result.LastFileEtag, maxEtags.LastFileEtag).ConfigureAwait(false);

            if (this.Options.Incremental)
            {
                var smugglerExportIncremental = await Operations.GetIncrementalExportKey().ConfigureAwait(false);
                smugglerExportIncremental.Destinations[betweenOptions.IncrementalKey] = incremental;

                await Operations.PutIncrementalExportKey(smugglerExportIncremental).ConfigureAwait(false); // importStore.AsyncFilesCommands.Configuration.SetKeyAsync<SmugglerExportIncremental>(SmugglerExportIncremental.RavenDocumentKey, smugglerExportIncremental);
            }   
        }

        private async Task<Etag> CopyBetweenStores(Etag lastEtag, Etag maxEtag)
        {
            var totalFiles = 0;
            var totalConfigurations = 0;
            var lastReport = SystemTime.UtcNow;
            var reportInterval = TimeSpan.FromSeconds(10);
            Operations.ShowProgress("Exporting Files");

            Exception exceptionHappened = null;
            try
            {
                while (true)
                {
                    bool hasConfigs = false;

                    foreach (var config in await Operations.GetConfigurations(totalConfigurations, Options.BatchSize).ConfigureAwait(false))
                    {
                        if (internalConfigs.IsMatch(config.Key))
                            continue;

                        hasConfigs = true;

                        await Operations.PutConfig(config.Key, EnsureValidExportConfig(config)).ConfigureAwait(false);

                        totalConfigurations++;

                        if (totalConfigurations % 100 == 0 || SystemTime.UtcNow - lastReport > reportInterval)
                        {
                            Operations.ShowProgress("Exported {0} configurations. ", totalConfigurations);
                            lastReport = SystemTime.UtcNow;
                        }
                    }

                    if (hasConfigs == false)
                        break;
                }

                Operations.ShowProgress("Done with reading configurations, total: {0}", totalConfigurations);

                using (var files = await Operations.GetFiles(lastEtag, Options.BatchSize).ConfigureAwait(false))
                {
                    while (await files.MoveNextAsync().ConfigureAwait(false))
                    {
                        var file = files.Current;
                        if (file.IsTombstone)
                            continue; // Skip if the file has been deleted.

                        var tempLastEtag = file.Etag;
                        if (maxEtag != null && tempLastEtag.CompareTo(maxEtag) > 0)
                            break;

                        if (Options.StripReplicationInformation)
                            file.Metadata = Operations.StripReplicationInformationFromMetadata(file.Metadata);

                        if (Options.ShouldDisableVersioningBundle)
                            file.Metadata = Operations.DisableVersioning(file.Metadata);

                        var downloadedFile = await Operations.DownloadFile(file).ConfigureAwait(false);
                        await Operations.PutFile( file, downloadedFile, file.TotalSize.Value ).ConfigureAwait(false);

                        lastEtag = tempLastEtag;

                        totalFiles++;
                        if (totalFiles % 1000 == 0 || SystemTime.UtcNow - lastReport > reportInterval)
                        {
                            //TODO: Show also the MB/sec and total GB exported.
                            Operations.ShowProgress("Exported {0} files. ", totalFiles);
                            lastReport = SystemTime.UtcNow;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Operations.ShowProgress("Got Exception during smuggler export. Exception: {0}. ", e.Message);
                Operations.ShowProgress("Done with reading files, total: {0}, lastEtag: {1}", totalFiles, lastEtag);

                exceptionHappened = new SmugglerExportException(e.Message, e)
                {
                    LastEtag = lastEtag,
                };
            }

            if (exceptionHappened != null)
                throw exceptionHappened;

            Operations.ShowProgress("Done with reading documents, total: {0}, lastEtag: {1}", totalFiles, lastEtag);
            return lastEtag;
        }
    }
}
#endif