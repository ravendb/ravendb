using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Abstractions.Util.Streams;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Abstractions.Smuggler
{
    public class SmugglerFilesApiBase : ISmugglerApi<FilesConnectionStringOptions, SmugglerFilesOptions, ExportFilesResult>
    {
        private enum CompressionEncoding
        {
            None,
            GZip,
            Defrate,
        };

        private class FileContainer
        {
            public string Key;
            public RavenJObject Metadata;
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

            var directory = result.FilePath;
            if (!Directory.Exists(result.FilePath))
            {
                if (!File.Exists(result.FilePath))
                {
                    Directory.CreateDirectory(result.FilePath);
                }
                else directory = Path.GetDirectoryName(result.FilePath);                    
            }

            if (Options.Incremental)
            {
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
            else
            {
                result.FilePath = Path.Combine(result.FilePath, SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture) + ".ravenfs-dump"); 
            }

            SmugglerExportException lastException = null;

            try
            {
                await DetectServerSupportedFeatures(exportOptions.From);
            }
            catch (WebException e)
            {
                throw new SmugglerExportException("Failed to query server for supported features. Reason : " + e.Message)
                {
                    LastEtag = Etag.Empty,
                    File = result.FilePath
                };
            }

            if (string.IsNullOrWhiteSpace(result.FilePath))
                throw new SmugglerException("Output directory cannot be null, empty or whitespace.");

            var stream = File.Create(result.FilePath);
            try
            {
                // used to synchronize max returned values for put/delete operations
                var maxEtags = Operations.FetchCurrentMaxEtags();

                try
                {
                    await ExportFiles(exportOptions, stream, result.LastFileEtag, maxEtags.LastFileEtag);
                }
                catch (SmugglerExportException ex)
                {
                    result.LastFileEtag = ex.LastEtag;
                    ex.File = result.FilePath;
                    lastException = ex;
                }
                catch (Exception)
                {
                    throw;
                }

                if (Options.Incremental)
                {
                    WriteLastEtagsToFile(result, directory);
                }

                if (lastException != null)
                    throw lastException;

                return result;
            }
            finally
            {
                stream.Dispose();
            }
        }

        private async Task<Etag> ExportFiles(SmugglerExportOptions<FilesConnectionStringOptions> options, Stream stream, Etag lastEtag, Etag maxEtag)
        {
            var totalCount = 0;
            var lastReport = SystemTime.UtcNow;
            var reportInterval = TimeSpan.FromSeconds(2);
            Operations.ShowProgress("Exporting Files");

            using (var archive = new ZipArchive(stream, ZipArchiveMode.Create))           
            {
                var metadataList = new List<FileContainer>();

                Exception exceptionHappened = null;

                try
                {
                    using (var files = await Operations.GetFiles(options.From, lastEtag, Math.Min(Options.BatchSize, int.MaxValue)))
                    {
                        while (await files.MoveNextAsync())
                        {
                            var file = files.Current;

                            var tempLastEtag = file.Etag;
                            if (maxEtag != null && tempLastEtag.CompareTo(maxEtag) > 0)
                                break;

                            // Write the metadata (which includes the stream size and file container name)
                            var fileContainer = new FileContainer
                            {
                                Key = Path.Combine(file.Directory.TrimStart('/'), file.Name),
                                Metadata = file.Metadata,
                            };

                            ZipArchiveEntry fileToStore = archive.CreateEntry(fileContainer.Key);

                            using (var fileStream = await Operations.DownloadFile(file))
                            using (var zipStream = fileToStore.Open())
                            {
                                await fileStream.CopyToAsync(zipStream).ConfigureAwait(false);
                            }

                            metadataList.Add(fileContainer);

                            lastEtag = tempLastEtag;

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
                catch (Exception e)
                {
                    Operations.ShowProgress("Got Exception during smuggler export. Exception: {0}. ", e.Message);
                    Operations.ShowProgress("Done with reading files, total: {0}, lastEtag: {1}", totalCount, lastEtag);
                    
                    exceptionHappened = new SmugglerExportException(e.Message, e)
                    {
                        LastEtag = lastEtag,
                    };
                }

                var metadataEntry = archive.CreateEntry(".metadata");
                using (var metadataStream = metadataEntry.Open())
                using (var writer = new StreamWriter(metadataStream))
                {
                    foreach (var item in metadataList)
                        writer.WriteLine(RavenJObject.FromObject(item));
                }

                if (exceptionHappened != null )
                    throw exceptionHappened;

                Operations.ShowProgress("Done with reading documents, total: {0}, lastEtag: {1}", totalCount, lastEtag);
                return lastEtag;
            }
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
            Operations.Configure(Options);
            Operations.Initialize(Options);

            await DetectServerSupportedFeatures(importOptions.To);

            if (Options.Incremental == false)
            {
                await ImportData(importOptions, importOptions.FromFile);
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
                await ImportData(importOptions, filename);
        }

        private async Task ImportData(SmugglerImportOptions<FilesConnectionStringOptions> importOptions, string filename)
        {
            var sw = Stopwatch.StartNew();
            var directory = Path.GetDirectoryName(filename);

            var serializer = JsonExtensions.CreateDefaultJsonSerializer();

            // We open the zip file. 
            using (var archive = new ZipArchive(File.OpenRead(filename), ZipArchiveMode.Read))
            {            
                var filesLookup = archive.Entries.ToDictionary( x => x.FullName );
                var metadataEntry = filesLookup[".metadata"];
                using ( var streamReader = new StreamReader(metadataEntry.Open()) )
                {
                    foreach (var json in streamReader.EnumerateJsonObjects())
                    {
                        // For each entry in the metadata file.                        
                        var container = serializer.Deserialize<FileContainer>(new StringReader(json));

                        var entry = filesLookup[container.Key];
                        using (var dataStream = entry.Open())
                        {
                            var header = new FileHeader(container.Key, container.Metadata);
                            await Operations.PutFiles(header, dataStream, entry.Length);
                        }

                        Options.CancelToken.Token.ThrowIfCancellationRequested();
                    }

                    Options.CancelToken.Token.ThrowIfCancellationRequested();
                }
            }

            sw.Stop();
        }


        public virtual Task Between(SmugglerBetweenOptions<FilesConnectionStringOptions> betweenOptions)
        {
            throw new NotImplementedException();
        }
    }
}
