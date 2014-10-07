using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Primitives;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Abstractions.Smuggler
{
    public class SmugglerFilesApiBase : ISmugglerApi<FilesConnectionStringOptions, SmugglerFilesOptions, ExportFilesResult>
    {
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

                result.FilePath = Path.Combine(result.FilePath, SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture) + ".ravenfs-incremental-dump");
                if (File.Exists(result.FilePath))
                {
                    var counter = 1;
                    while (true)
                    {
                        // ReSharper disable once AssignNullToNotNullAttribute
                        result.FilePath = Path.Combine(Path.GetDirectoryName(result.FilePath), SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture) + " - " + counter + ".ravenfs-incremental-dump");

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
                await DetectServerSupportedFeatures(exportOptions.From);
            }
            catch (WebException e)
            {
                throw new SmugglerExportException("Failed to query server for supported features. Reason : " + e.Message)
                {
                    LastEtag = Etag.Empty,
                    File = ownedStream ? result.FilePath : null
                };
            }

            try
            {
                using (var gZipStream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true))
                using (var streamWriter = new StreamWriter(gZipStream))
                {
                    // used to synchronize max returned values for put/delete operations
                    var maxEtags = Operations.FetchCurrentMaxEtags();

                    try
                    {
                        await ExportFiles(exportOptions, streamWriter, result.LastFileEtag, maxEtags.LastFileEtag);
                    }
                    catch (SmugglerExportException e)
                    {
                        result.LastFileEtag = e.LastEtag;
                        e.File = ownedStream ? result.FilePath : null;
                        lastException = e;
                    }
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

        private async Task<Etag> ExportFiles(SmugglerExportOptions<FilesConnectionStringOptions> options, StreamWriter metadataStreamWriter, Etag lastEtag, Etag maxEtag)
        {
            Operations.Configure(Options);
            Operations.Initialize(Options);

            var totalCount = 0;
            var lastReport = SystemTime.UtcNow;
            var reportInterval = TimeSpan.FromSeconds(2);
            Operations.ShowProgress("Exporting Files");

            while (true)
            {
                bool hasFiles = false;
                try
                {
                    var maxRecords = Options.Limit - totalCount;
                    if (maxRecords > 0)
                    {
                        using (var files = await Operations.GetFiles(options.From, lastEtag, Math.Min(Options.BatchSize, maxRecords)))
                        {
                            while (await files.MoveNextAsync())
                            {
                                hasFiles = true;
                                var file = files.Current;

                                var tempLastEtag = file.Etag;
                                if (maxEtag != null && tempLastEtag.CompareTo(maxEtag) > 0)
                                    break;

                                lastEtag = tempLastEtag;                                

                                // Retrieve the file and write it to the stream. 
                                var fileStream = await Operations.DownloadFile(file);
                                
                                // Write the metadata (which includes the stream size and file container name)
                                metadataStreamWriter.Write( RavenJObject.FromObject(file) );

                                totalCount++;
                                if (totalCount % 1000 == 0 || SystemTime.UtcNow - lastReport > reportInterval)
                                {
                                    //TODO: Show also the MB/sec and total GB exported.
                                    Operations.ShowProgress("Exported {0} files. ", totalCount);
                                    lastReport = SystemTime.UtcNow;
                                }
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    Operations.ShowProgress("Got Exception during smuggler export. Exception: {0}. ", e.Message);
                    Operations.ShowProgress("Done with reading files, total: {0}, lastEtag: {1}", totalCount, lastEtag);
                    throw new SmugglerExportException(e.Message, e)
                    {
                        LastEtag = lastEtag,
                    };
                }

                Operations.ShowProgress("Done with reading documents, total: {0}, lastEtag: {1}", totalCount, lastEtag);
                return lastEtag;
            }


            throw new NotImplementedException();
        }

        private async Task DetectServerSupportedFeatures(FilesConnectionStringOptions filesConnectionStringOptions)
        {
            var serverVersion = await this.Operations.GetVersion(filesConnectionStringOptions);
            if (string.IsNullOrEmpty(serverVersion))
                throw new SmugglerExportException("Server version is not available.");

            var smugglerVersion = FileVersionInfo.GetVersionInfo(AssemblyHelper.GetAssemblyLocationFor<SmugglerFilesApiBase>()).ProductVersion;
            var subServerVersion = serverVersion.Substring(0, 3);
            var subSmugglerVersion = smugglerVersion.Substring(0, 3);

            var intServerVersion = int.Parse(subServerVersion.Replace(".", string.Empty));
            if (intServerVersion < 30)
                throw new SmugglerExportException(string.Format("File Systems are not available on Server version: {0}. Smuggler version: {1}.", subServerVersion, subSmugglerVersion));
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
            // ReSharper disable once AssignNullToNotNullAttribute
            var etagFileLocation = Path.Combine(Path.GetDirectoryName(backupPath), IncrementalExportStateFile);
            using (var streamWriter = new StreamWriter(File.Create(etagFileLocation)))
            {
                new RavenJObject
					{
						{"LastFileEtag", result.LastFileEtag.ToString()},
                        {"LastDeletedFileEtag", result.LastDeletedFileEtag.ToString()},
					}.WriteTo(new JsonTextWriter(streamWriter));
            }
        }

        public virtual Task ImportData(SmugglerImportOptions<FilesConnectionStringOptions> importOptions)
        {
            throw new NotImplementedException();
        }

        public virtual Task Between(SmugglerBetweenOptions<FilesConnectionStringOptions> betweenOptions)
        {
            throw new NotImplementedException();
        }
    }
}
