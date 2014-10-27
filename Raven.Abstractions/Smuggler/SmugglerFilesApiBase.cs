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
            public string Filename;
            public FileHeader Metadata;
            public long Size;

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
                using (var gZipStream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true))
                using (var streamWriter = new StreamWriter(gZipStream))
                {
                    // used to synchronize max returned values for put/delete operations
                    var maxEtags = Operations.FetchCurrentMaxEtags();

                    try
                    {
                        await ExportFiles(exportOptions, result.FilePath, streamWriter, result.LastFileEtag, maxEtags.LastFileEtag);
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

        private async Task<Etag> ExportFiles(SmugglerExportOptions<FilesConnectionStringOptions> options, string filenamePrefix, StreamWriter metadataStreamWriter, Etag lastEtag, Etag maxEtag)
        {
            var totalCount = 0;
            var lastReport = SystemTime.UtcNow;
            var reportInterval = TimeSpan.FromSeconds(2);
            Operations.ShowProgress("Exporting Files");

            string filename = filenamePrefix + ".1.data";
            using (var stream = File.Create(filename))
            using (var afterCompressionCountingStream = new CountingStream(stream))
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

                                    // Retrieve the file and write it to the stream. 
                                    // TODO: We are wasting precious resources decompressing and then compressing again. 
                                    long startPosition = afterCompressionCountingStream.NumberOfWrittenBytes;
                                    long totalFileSize = 0;

                                    using (var fileStream = await Operations.DownloadFile(file))
                                    using (var beforeCompressionCountingStream = new CountingStream(fileStream))
                                    using (var gZipStream = new GZipStream(afterCompressionCountingStream, CompressionMode.Compress, leaveOpen: true))            
                                    {
                                        await beforeCompressionCountingStream.CopyToAsync(gZipStream).ConfigureAwait(false);
                                        totalFileSize = beforeCompressionCountingStream.NumberOfReadBytes;
                                    }
                                    long endPosition = afterCompressionCountingStream.NumberOfWrittenBytes;

                                    // Write the metadata (which includes the stream size and file container name)
                                    var fileContainer = new FileContainer
                                    {
                                        Filename = Path.GetFileName(filename),                                        
                                        Start = startPosition,
                                        Length = endPosition - startPosition,
                                        Size = totalFileSize,
                                        Metadata = file,
                                        Encoding = CompressionEncoding.GZip,  
                                    };

                                    metadataStreamWriter.WriteLine(RavenJObject.FromObject(fileContainer));

                                    lastEtag = tempLastEtag;

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
            // We open the metadata file
            using (var stream = File.OpenRead(filename))
            {
                var sw = Stopwatch.StartNew();

                var directory = Path.GetDirectoryName(filename);

                // Try to read the stream compressed.
                var sizeStream = new CountingStream(new GZipStream(stream, CompressionMode.Decompress));
                var streamReader = new StreamReader(sizeStream);
                
                var serializer = JsonExtensions.CreateDefaultJsonSerializer();                               

                // We will store and lock for read every data file used by smuggler.
                var dataStreams = new Dictionary<string, Stream>();
                try
                {
                    foreach (var json in streamReader.EnumerateJsonObjects() )
                    {
                        // For each entry in the metadata file.                        
                        var container = serializer.Deserialize<FileContainer>(new StringReader(json));

                        // We create the data stream if it hasn't been created before.
                        Stream wholeDataStream;
                        if (!dataStreams.TryGetValue(container.Filename, out wholeDataStream))
                        {
                            wholeDataStream = File.OpenRead(Path.Combine(directory, container.Filename));
                            dataStreams[container.Filename] = wholeDataStream;
                        }

                        wholeDataStream.Seek(container.Start, SeekOrigin.Begin);

                        // We seek to the location and read the data, no matter the encoding it was saved.
                        var substream = new Substream(wholeDataStream, (int)container.Start, (int)container.Length);

                        Stream dataStream;
                        switch (container.Encoding)
                        {
                            case CompressionEncoding.GZip:
                                dataStream = new GZipStream(substream, CompressionMode.Decompress, true);
                                break;
                            case CompressionEncoding.Defrate:
                                dataStream = new DeflateStream(substream, CompressionMode.Decompress, true);
                                break;
                            case CompressionEncoding.None:
                                dataStream = substream;
                                break;   
                            default:
                                throw new InvalidEnumArgumentException("Cannot read data file. Unrecognized encoding: " + container.Encoding);
                        }

                        await Operations.PutFiles(container.Metadata, dataStream, container.Size);

                        Options.CancelToken.Token.ThrowIfCancellationRequested();
                    }
                }
                finally
                {
                    // We get rid of every data stream open during the process.
                    foreach (var item in dataStreams)
                        item.Value.Dispose();
                }

                Options.CancelToken.Token.ThrowIfCancellationRequested();

                sw.Stop();
            }
        }


        public virtual Task Between(SmugglerBetweenOptions<FilesConnectionStringOptions> betweenOptions)
        {
            throw new NotImplementedException();
        }
    }
}
