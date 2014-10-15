using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Primitives;
using System.Data.Odbc;
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
            public FileHeader Metadata;
            public string Filename;
            public long Start;
            public long Length;
            public CompressionEncoding Encoding;
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

            if (!Directory.Exists(result.FilePath))
            {
                if (!File.Exists(result.FilePath))
                    Directory.CreateDirectory(result.FilePath);
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
                using (var gZipStream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true))
                using (var streamWriter = new StreamWriter(gZipStream))
                {
                    // used to synchronize max returned values for put/delete operations
                    var maxEtags = Operations.FetchCurrentMaxEtags();

                    try
                    {
                        await ExportFiles(exportOptions, result.FilePath, streamWriter, result.LastFileEtag, maxEtags.LastFileEtag);
                    }
                    catch (SmugglerExportException e)
                    {
                        result.LastFileEtag = e.LastEtag;
                        e.File = result.FilePath;
                        lastException = e;
                    }
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

        private async Task<Etag> ExportFiles(SmugglerExportOptions<FilesConnectionStringOptions> options, string filenamePrefix, StreamWriter metadataStreamWriter, Etag lastEtag, Etag maxEtag)
        {
            var totalCount = 0;
            var lastReport = SystemTime.UtcNow;
            var reportInterval = TimeSpan.FromSeconds(2);
            Operations.ShowProgress("Exporting Files");

            string filename = filenamePrefix + ".1.data";
            using (var stream = File.Create(filename))
            using (var countingStream = new CountingStream(stream))
            using (var gZipStream = new GZipStream(countingStream, CompressionMode.Compress, leaveOpen: true))            
            {
                while (true)
                {
                    try
                    {
                        var maxRecords = Options.Limit - totalCount;
                        if (maxRecords > 0)
                        {
                            using (var files = await Operations.GetFiles(options.From, lastEtag, Math.Min(Options.BatchSize, maxRecords)))
                            {
                                while (await files.MoveNextAsync())
                                {
                                    var file = files.Current;

                                    var tempLastEtag = file.Etag;
                                    if (maxEtag != null && tempLastEtag.CompareTo(maxEtag) > 0)
                                        break;

                                    lastEtag = tempLastEtag;

                                    // Retrieve the file and write it to the stream. 
                                    // TODO: We are wasting precious resources decompressing and then compressing again. 
                                    long startPosition = countingStream.NumberOfWrittenBytes;
                                    using (var fileStream = await Operations.DownloadFile(file))
                                    {
                                        await fileStream.CopyToAsync(gZipStream).ConfigureAwait(false);
                                    }
                                    long endPosition = countingStream.NumberOfWrittenBytes;

                                    // Write the metadata (which includes the stream size and file container name)
                                    var fileContainer = new FileContainer
                                    {
                                        Filename = Path.GetFileName(filename),                                        
                                        Start = startPosition,
                                        Length = endPosition - startPosition,
                                        Metadata = file,
                                        Encoding = CompressionEncoding.GZip,
                                    };

                                    metadataStreamWriter.Write(RavenJObject.FromObject(fileContainer));

                                    totalCount++;
                                    if (totalCount%1000 == 0 || SystemTime.UtcNow - lastReport > reportInterval)
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

            var files = Directory.GetFiles(Path.GetFullPath(importOptions.FromFile))
                            .Where(file => ".ravenfs-incremental-dump".Equals(Path.GetExtension(file), StringComparison.CurrentCultureIgnoreCase))
                            .OrderBy(File.GetLastWriteTimeUtc)
                            .ToArray();

            if (files.Length == 0)
                return;

            foreach (string filename in files)
                await ImportData(importOptions, filename);
        }

        private async Task ImportData(SmugglerImportOptions<FilesConnectionStringOptions> importOptions, string file)
        {
            using (var stream = File.OpenRead(importOptions.FromFile))
            {
                var sw = Stopwatch.StartNew();

                // Try to read the stream compressed, otherwise continue uncompressed.
                stream.Position = 0;
                var sizeStream = new CountingStream(new GZipStream(stream, CompressionMode.Decompress));
                var streamReader = new StreamReader(sizeStream);

                var jsonReader = new JsonTextReader(streamReader);
                if (jsonReader.Read() == false)
                    return;

                if (jsonReader.TokenType != JsonToken.StartObject)
                    throw new InvalidDataException("StartObject was expected");

                var importCounts = new Dictionary<string, int>();
                var importSectionRegistar = new Dictionary<string, Func<int>>();

                Options.CancelToken.Token.ThrowIfCancellationRequested();

                importSectionRegistar.Add("Files", () =>
                {
                    Operations.ShowProgress("Begin reading files");
                    var filesCount = ImportFiles(importOptions, jsonReader).Result;
                    Operations.ShowProgress(string.Format("Done with reading files, total: {0}", filesCount));
                    return filesCount;
                });

                importSectionRegistar.Keys.ForEach(k => importCounts[k] = 0);

                while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndObject)
                {
                    Options.CancelToken.Token.ThrowIfCancellationRequested();

                    if (jsonReader.TokenType != JsonToken.PropertyName)
                        throw new InvalidDataException("PropertyName was expected");
                                        
                    var currentSection = jsonReader.Value.ToString();

                    Func<int> currentAction;
                    if (importSectionRegistar.TryGetValue(currentSection, out currentAction) == false)
                        throw new InvalidDataException("Unexpected property found: " + jsonReader.Value);

                    if (jsonReader.Read() == false)
                    {
                        importCounts[currentSection] = 0;
                        continue;
                    }

                    if (jsonReader.TokenType != JsonToken.StartArray)
                        throw new InvalidDataException("StartArray was expected");

                    importCounts[currentSection] = currentAction();
                }

                sw.Stop();

                Operations.ShowProgress("Imported {0:#,#;;0} documents and {1:#,#;;0} attachments, deleted {2:#,#;;0} documents and {3:#,#;;0} attachments in {4:#,#.###;;0} s", exportCounts["Docs"], exportCounts["Attachments"], exportCounts["DocsDeletions"], exportCounts["AttachmentsDeletions"], sw.ElapsedMilliseconds / 1000f);
            }



            throw new NotImplementedException();
        }

        private async Task<int> ImportFiles(SmugglerImportOptions<FilesConnectionStringOptions> options, JsonTextReader jsonReader)
        {
            var count = 0;

            while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
            {
                Options.CancelToken.Token.ThrowIfCancellationRequested();

                var item = RavenJToken.ReadFrom(jsonReader);
               


                // Operations.ShowProgress("Importing file {0}", attachmentExportInfo.Key);

                // await Operations.PutAttachment(dst, attachmentExportInfo);

                count++;
            }

            // await Operations.PutAttachment(dst, null); // force flush

            return count;
        }

        public virtual Task Between(SmugglerBetweenOptions<FilesConnectionStringOptions> betweenOptions)
        {
            throw new NotImplementedException();
        }
    }
}
