using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.MEF;
using Raven.Bundles.Compression.Plugin;
using Raven.Bundles.Encryption.Plugin;
using Raven.Bundles.Encryption.Settings;
using Raven.Database.Config;
using Raven.Database.FileSystem;
using Raven.Database.FileSystem.Bundles.Encryption.Plugin;
using Raven.Database.FileSystem.Infrastructure;
using Raven.Database.FileSystem.Util;
using Raven.Database.Impl;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Database.Util;

namespace Raven.StorageExporter
{
    public class StorageExporter : IDisposable
    {
        public StorageExporter(string databaseBaseDirectory, string databaseOutputFile, 
            int batchSize, Etag documentsStartEtag, bool hasCompression, EncryptionConfiguration encryption, string journalsPath, bool isRavenFs)
        {
            HasCompression = hasCompression;
            Encryption = encryption;
            baseDirectory = databaseBaseDirectory;
            outputDirectory = databaseOutputFile;
            var ravenConfiguration = new RavenConfiguration
            {
                DataDirectory = databaseBaseDirectory,
                CacheDocumentsInMemory = false,
                Storage =
                {
                    PreventSchemaUpdate = true,
                    SkipConsistencyCheck = true,
                    Voron =
                    {
                        JournalsStoragePath = journalsPath
                    },
                    Esent =
                    {
                        JournalsStoragePath = journalsPath
                    }
                }
            };
            if (isRavenFs)
            {
                ravenConfiguration.FileSystem.DataDirectory = databaseBaseDirectory;
            }
            CreateTransactionalStorage(ravenConfiguration, isRavenFs);
            this.batchSize = batchSize;
            DocumentsStartEtag = documentsStartEtag;
        }

        public Etag DocumentsStartEtag { get; set; }

        public bool HasCompression { get; set; }

        public EncryptionConfiguration Encryption { get; set; }

        public void ExportFilesystemAsAttachments()
        {
            using (var stream = File.Create(outputDirectory))
            using (var gZipStream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true))
            using (var streamWriter = new StreamWriter(gZipStream))
            {
                var jsonWriter = new JsonTextWriter(streamWriter)
                {
                    Formatting = Formatting.Indented
                };
                jsonWriter.WriteStartObject();
                //Attachments
                jsonWriter.WritePropertyName("Attachments");
                jsonWriter.WriteStartArray();
                WriteFilesAsAttachments(jsonWriter);
                jsonWriter.WriteEndArray();
                jsonWriter.WriteEndObject();
                streamWriter.Flush();
            }
        }

        public void ExportDatabase()
        {
           
            using (var stream = File.Create(outputDirectory))
            using (var gZipStream = new GZipStream(stream, CompressionMode.Compress,leaveOpen: true))
            using (var streamWriter = new StreamWriter(gZipStream))
            {
                var jsonWriter = new JsonTextWriter(streamWriter)
                {
                    Formatting = Formatting.Indented
                };
                jsonWriter.WriteStartObject();
                //Indexes
                jsonWriter.WritePropertyName("Indexes");
                jsonWriter.WriteStartArray();
                WriteIndexes(jsonWriter);
                jsonWriter.WriteEndArray();
                //Documents
                jsonWriter.WritePropertyName("Docs");
                jsonWriter.WriteStartArray();
                WriteDocuments(jsonWriter);
                jsonWriter.WriteEndArray();
                //Attachments
                jsonWriter.WritePropertyName("Attachments");
                jsonWriter.WriteStartArray();
                WriteAttachments(jsonWriter);
                jsonWriter.WriteEndArray();
                //Transformers
                jsonWriter.WritePropertyName("Transformers");
                jsonWriter.WriteStartArray();
                WriteTransformers(jsonWriter);
                jsonWriter.WriteEndArray();
                //Identities
                jsonWriter.WritePropertyName("Identities");
                jsonWriter.WriteStartArray();
                WriteIdentities(jsonWriter);
                jsonWriter.WriteEndArray();
                //end of export
                jsonWriter.WriteEndObject();
                streamWriter.Flush();
            }
        }

        private void ReportCompletedNoMoreItemsFound(string stage, long from, long outof)
        {
            ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.Green, "Completed exporting {0} out of {1} {2} - no more {2} has been found", from, outof, stage);
        }

        private void ReportProgress(string stage, long from, long outof)
        {
            if (from == outof)
            {
                ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.Green, "Completed exporting {0} out of {1} {2}", from, outof, stage);
            }
            else
            {
                Console.WriteLine("exporting {0} out of {1} {2}", from, outof, stage);
            }
        }

        private static void ReportCorrupted(string stage, long currentDocsCount, string error)
        {
            ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.Red,
                "Failed to export {0} number {1}, skipping it, error: {2}", 
                stage, currentDocsCount, error);
        }

        private static void ReportCorruptedDocumentWithEtag(string stage, long currentDocsCount, string error, Etag currentLastEtag)
        {
            ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.Red,
                "Failed to export {0}: document number {1} with etag {2} failed to export, skipping it, error: {3}",
                stage, currentDocsCount, currentLastEtag, error);
        }

        private void WriteDocuments(JsonTextWriter jsonWriter)
        {
            long totalDocsCount = 0;
            

            storage.Batch(accsesor => totalDocsCount = accsesor.Documents.GetDocumentsCount());

            using (DocumentCacher.SkipSetDocumentsInDocumentCache())
            {
                if (DocumentsStartEtag == Etag.Empty)
                {
                    ExtractDocuments(jsonWriter, totalDocsCount);
                }
                else
                {
                    ExtractDocumentsFromEtag(jsonWriter, totalDocsCount);
                }
            }  
        }

        private void ExtractDocuments(JsonTextWriter jsonWriter, long totalDocsCount)
        {
            long currentDocsCount = 0;
            do
            {
                var previousDocsCount = currentDocsCount;

                try
                {
                    var foundDocs = false;

                    storage.Batch(accsesor =>
                    {
                        var docs = accsesor.Documents.GetDocuments(start: (int) currentDocsCount);
                        foreach (var doc in docs)
                        {
                            foundDocs = true;

                            doc.ToJson(true).WriteTo(jsonWriter);
                            currentDocsCount++;

                            if (currentDocsCount % batchSize == 0)
                                ReportProgress("documents", currentDocsCount, totalDocsCount);
                        }
                    });

                    if (foundDocs == false)
                    {
                        ReportCompletedNoMoreItemsFound("documents", currentDocsCount, totalDocsCount);
                        break;
                    }
                }
                catch (Exception e)
                {
                    currentDocsCount++;
                    ReportCorrupted("document", currentDocsCount, e.Message);
                }
                finally
                {
                    if (currentDocsCount > previousDocsCount)
                        ReportProgress("documents", currentDocsCount, totalDocsCount);
                }
            } while (currentDocsCount < totalDocsCount);
        }

        private void ExtractDocumentsFromEtag(JsonTextWriter jsonWriter, long totalDocsCount)
        {
            Debug.Assert(DocumentsStartEtag != Etag.Empty);

            var currentLastEtag = DocumentsStartEtag;
            ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.Yellow, "Starting to export documents as of etag={0}\n" +
                    "Total documents count doesn't substract skipped items\n", DocumentsStartEtag);
            currentLastEtag = currentLastEtag.DecrementBy(1);

            var ct = new CancellationToken();
            long currentDocsCount = 0;
            do
            {
                var previousDocsCount = currentDocsCount;
                try
                {
                    var foundDocs = false;

                    storage.Batch(accsesor =>
                    {
                        var docs = accsesor.Documents.GetDocumentsAfter(currentLastEtag, batchSize, ct);
                        foreach (var doc in docs)
                        {
                            foundDocs = true;

                            doc.ToJson(true).WriteTo(jsonWriter);
                            currentDocsCount++;
                            currentLastEtag = doc.Etag;

                            if (currentDocsCount % batchSize == 0)
                                ReportProgress("documents", currentDocsCount, totalDocsCount);
                        }
                    });

                    if (foundDocs == false)
                    {
                        ReportCompletedNoMoreItemsFound("documents", currentDocsCount, totalDocsCount);
                        break;
                    }
                }
                catch (Exception e)
                {
                    currentDocsCount++;
                    currentLastEtag = currentLastEtag.IncrementBy(1);
                    ReportCorruptedDocumentWithEtag("documents", currentDocsCount, e.Message, currentLastEtag);
                }
                finally
                {
                    if (currentDocsCount > previousDocsCount)
                        ReportProgress("documents", currentDocsCount, totalDocsCount);
                }
            } while (currentDocsCount > totalDocsCount);
        }

        private void WriteTransformers(JsonTextWriter jsonWriter)
        {
            var indexDefinitionsBasePath = Path.Combine(baseDirectory, indexDefinitionFolder);
            var transformers = Directory.GetFiles(indexDefinitionsBasePath, "*.transform");
            var currentTransformerCount = 0;
            foreach (var file in transformers)
            {
                var ravenObj = RavenJObject.Parse(File.ReadAllText(file));
                jsonWriter.WriteStartObject();
                jsonWriter.WritePropertyName("name");
                jsonWriter.WriteValue(ravenObj.Value<string>("Name"));
                jsonWriter.WritePropertyName("definition");
                ravenObj.WriteTo(jsonWriter);
                jsonWriter.WriteEndObject();
                currentTransformerCount++;
                ReportProgress("transformers", currentTransformerCount, transformers.Count());
            }
        }

        private void WriteIndexes(JsonTextWriter jsonWriter)
        {
            var indexDefinitionsBasePath = Path.Combine(baseDirectory, indexDefinitionFolder);
            var indexes = Directory.GetFiles(indexDefinitionsBasePath, "*.index");
            int currentIndexCount = 0;
            foreach (var file in indexes)
            {
                var ravenObj = RavenJObject.Parse(File.ReadAllText(file));
                jsonWriter.WriteStartObject();
                jsonWriter.WritePropertyName("name");
                jsonWriter.WriteValue(ravenObj.Value<string>("Name"));
                jsonWriter.WritePropertyName("definition");
                ravenObj.WriteTo(jsonWriter);
                jsonWriter.WriteEndObject();
                currentIndexCount++;
                ReportProgress("indexes", currentIndexCount, indexes.Count());
            }
        }

        private void WriteIdentities(JsonTextWriter jsonWriter)
        {
            long totalIdentities = 0;
            var currentIdentitiesCount = 0;
            do
            {
                var foundIdentities = false;

                storage.Batch(accsesor =>
                {
                    var identities = accsesor.General.GetIdentities(currentIdentitiesCount, batchSize, out totalIdentities);
                    var filteredIdentities = identities.Where(x => FilterIdentity(x.Key));
                    foreach (var identityInfo in filteredIdentities)
                    {
                        foundIdentities = true;

                        new RavenJObject
                        {
                            {"Key", identityInfo.Key},
                            {"Value", identityInfo.Value}
                        }.WriteTo(jsonWriter);
                    }
                    currentIdentitiesCount += identities.Count();
                    ReportProgress("identities", currentIdentitiesCount, totalIdentities);
                });

                if (foundIdentities == false)
                {
                    ReportCompletedNoMoreItemsFound("identities", currentIdentitiesCount, totalIdentities);
                    break;
                }

            } while (totalIdentities > currentIdentitiesCount);
        }

        private List<string> _filteredFilesystemProperties = new List<string>
            {
                "Raven-Synchronization-History" , "Raven-Synchronization-Version", "Raven-Synchronization-Source", "ETag", "Raven-Creation-Date" , "Raven-Last-Modified" , "Origin" , "Content-MD5", "RavenFS-Size"
            };

        private void WriteFilesAsAttachments(JsonTextWriter jsonWriter)
        {
            int totalFilesCount = 0;
            fileStorage.Batch(accsesor =>
            {
                totalFilesCount = accsesor.GetFileCount();
            });
            if(totalFilesCount == 0)
                return;
            int currentFilesCount = 0;
            int previousFilesCount = 0;
            var lastEtag = Etag.Empty;
            
            do
            {                
                try
                {
                    previousFilesCount = currentFilesCount;
                    var hadFiles = false;

                    fileStorage.Batch(accsesor =>
                    {
                        var fileHeaders = accsesor.GetFilesAfter(lastEtag, batchSize);

                        foreach (var header in fileHeaders)
                        {      
                            hadFiles = true;

                            if(ShouldSkipFile(header))
                                continue;
                            var file = StorageStream.Reading(fileStorage, header.FullPath);
                            jsonWriter.WriteStartObject();
                            jsonWriter.WritePropertyName("Data");
                            WriteFileAsBase64(jsonWriter, file);
                            jsonWriter.WritePropertyName("Metadata");
                            var fileEtag = header.Etag;
                            var metadata = FilterMetadataProperties(header.Metadata);
                            metadata.WriteTo(jsonWriter);
                            jsonWriter.WritePropertyName("Key");
                            var key = RemoveSlashIfNeeded(header.FullPath);
                            jsonWriter.WriteValue(key);
                            jsonWriter.WritePropertyName("Etag");
                            jsonWriter.WriteNull();
                            jsonWriter.WriteEndObject();
                            lastEtag = fileEtag;
                            currentFilesCount++;
                            if (currentFilesCount % batchSize == 0)
                                ReportProgress("files", currentFilesCount, totalFilesCount);
                        }
                    });

                    if (hadFiles == false)
                        break;
                }
                catch (Exception e)
                {
                    lastEtag = lastEtag.IncrementBy(1);
                    currentFilesCount++;
                    ReportCorrupted("files", currentFilesCount, e.Message);
                }
                finally
                {
                    if (currentFilesCount > previousFilesCount)
                        ReportProgress("files", currentFilesCount, totalFilesCount);
                }
            } while (currentFilesCount < totalFilesCount);
        }

        private bool ShouldSkipFile(FileHeader header)
        {
            return header.Name.EndsWith(".deleting") || header.Name.EndsWith(".downloading") || header.IsTombstone;
        }

        private MemoryStream _buffer = new MemoryStream();
        private void WriteFileAsBase64(JsonTextWriter jsonWriter, StorageStream file)
        {
            _buffer.SetLength(0);
            file.CopyTo(_buffer);

            var base64String = Convert.ToBase64String(_buffer.GetBuffer(), 0, (int)_buffer.Length, Base64FormattingOptions.None);
            jsonWriter.WriteValue(base64String);
        }

        private string RemoveSlashIfNeeded(string headerFullPath)
        {
            if (headerFullPath.StartsWith("/"))
                return headerFullPath.Substring(1);
            return headerFullPath;
        }

        private RavenJObject FilterMetadataProperties(RavenJObject headerMetadata, bool rec = false)
        {
            try
            {
                foreach (var p in _filteredFilesystemProperties)
                {
                    headerMetadata.Remove(p);
                }
            }
            //If we are working on a snapshot we can't modify it, not sure it can happen but better safe than sorry
            catch (InvalidOperationException)
            {
                if (rec)
                    throw;
                return FilterMetadataProperties(headerMetadata.CloneToken() as RavenJObject, true);
            }
            return headerMetadata;
        }

        private void WriteAttachments(JsonTextWriter jsonWriter)
        {
            long totalAttachmentsCount = 0;
            storage.Batch(accsesor => totalAttachmentsCount = accsesor.Attachments.GetAttachmentsCount());
            if (totalAttachmentsCount == 0)
                return;

            var lastEtag = Etag.Empty;
            long currentAttachmentsCount = 0;
            do
            {
                var previousAttachmentCount = currentAttachmentsCount;

                try
                {
                    var foundAttachments = false;

                    storage.Batch(accsesor =>
                    {
                        var attachments = accsesor.Attachments.GetAttachmentsAfter(lastEtag, batchSize, long.MaxValue);
                        foreach (var attachmentInformation in attachments)
                        {
                            foundAttachments = true;

                            var attachment = accsesor.Attachments.GetAttachment(attachmentInformation.Key);
                            if (attachment == null)
                            {
                                ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.Red, "Couldn't find attachment '{0}'", attachmentInformation.Key);
                                continue;
                            }

                            var data = attachment.Data;
                            attachment.Data = () =>
                            {
                                var memoryStream = new MemoryStream();
                                storage.Batch(accessor => data().CopyTo(memoryStream));
                                memoryStream.Position = 0;
                                return memoryStream;
                            };

                            var attachmentData = attachment.Data().ReadData();
                            if (attachmentData == null)
                            {
                                ConsoleUtils.ConsoleWriteLineWithColor(ConsoleColor.Red, "No data was found for attachment '{0}'", attachment.Key);
                                continue;
                            }
                            var ravenJsonObj = new RavenJObject
                            {
                                { "Data", attachmentData },
                                { "Metadata", attachmentInformation.Metadata },
                                { "Key", attachmentInformation.Key },
                                { "Etag", new RavenJValue(attachmentInformation.Etag.ToString()) }
                            };
                            ravenJsonObj.WriteTo(jsonWriter);

                            lastEtag = attachmentInformation.Etag;
                            currentAttachmentsCount++;
                            if (currentAttachmentsCount % batchSize == 0)
                                ReportProgress("attachments", currentAttachmentsCount, totalAttachmentsCount);
                        }
                    });

                    if (foundAttachments == false)
                    {
                        ReportCompletedNoMoreItemsFound("attachments", currentAttachmentsCount, totalAttachmentsCount);
                        break;
                    }
                }
                catch (Exception e)
                {
                    lastEtag = lastEtag.IncrementBy(1);
                    currentAttachmentsCount++;
                    ReportCorrupted("attachment", currentAttachmentsCount, e.Message);
                }
                finally
                {
                    if (currentAttachmentsCount > previousAttachmentCount)
                        ReportProgress("attachments", currentAttachmentsCount, totalAttachmentsCount);
                }
            } while (currentAttachmentsCount < totalAttachmentsCount);
        }

        public bool FilterIdentity(string identityName)
        {
            if ("Raven/Etag".Equals(identityName, StringComparison.OrdinalIgnoreCase))
                return false;

            if ("IndexId".Equals(identityName, StringComparison.OrdinalIgnoreCase))
                return false;

            if (Constants.RavenSubscriptionsPrefix.Equals(identityName, StringComparison.OrdinalIgnoreCase))
                return false;

            return false;
        }

        private void CreateTransactionalStorage(InMemoryRavenConfiguration ravenConfiguration, bool isRavenFs)
        {
            if (string.IsNullOrEmpty(ravenConfiguration.DataDirectory) == false && Directory.Exists(ravenConfiguration.DataDirectory))
            {
                try
                {
                    if (TryToCreateTransactionalStorage(ravenConfiguration, HasCompression, Encryption, isRavenFs, out storage, out fileStorage) == false)
                        ConsoleUtils.PrintErrorAndFail("Failed to create transactional storage");
                }
                catch (UnauthorizedAccessException uae)
                {
                    ConsoleUtils.PrintErrorAndFail(string.Format("Failed to initialize the storage it is probably been locked by RavenDB.\nError message:\n{0}", uae.Message), uae.StackTrace);
                }
                catch (InvalidOperationException ioe)
                {
                    ConsoleUtils.PrintErrorAndFail(string.Format("Failed to initialize the storage it is probably been locked by RavenDB.\nError message:\n{0}", ioe.Message), ioe.StackTrace);
                }
                catch (Exception e)
                {
                    ConsoleUtils.PrintErrorAndFail(e.Message, e.StackTrace);
                    return;
                }

                return;
            }

            ConsoleUtils.PrintErrorAndFail(string.Format("Could not detect storage file under the given directory:{0}", ravenConfiguration.DataDirectory));
        }

        public static bool TryToCreateTransactionalStorage(InMemoryRavenConfiguration ravenConfiguration,
            bool hasCompression, EncryptionConfiguration encryption, bool isRavenFs, 
            out ITransactionalStorage storage, out Database.FileSystem.Storage.ITransactionalStorage fileStorage)
        {
            storage = null;
            fileStorage = null;
            if (File.Exists(Path.Combine(ravenConfiguration.DataDirectory, Voron.Impl.Constants.DatabaseFilename)))
            {
                if (isRavenFs)
                {
                    fileStorage = RavenFileSystem.CreateTransactionalStorage(ravenConfiguration);
                }
                else
                {
                    storage = ravenConfiguration.CreateTransactionalStorage(InMemoryRavenConfiguration.VoronTypeName, () => { }, () => { });
                }
            }
            else if (File.Exists(Path.Combine(ravenConfiguration.DataDirectory, "Data.ravenfs"))||
                     File.Exists(Path.Combine(ravenConfiguration.DataDirectory, "Data")))
            {
                if (isRavenFs)
                {
                    fileStorage = RavenFileSystem.CreateTransactionalStorage(ravenConfiguration);
                }
                else
                {
                    storage = ravenConfiguration.CreateTransactionalStorage(InMemoryRavenConfiguration.EsentTypeName, () => { }, () => { });
                }
            }
            if (storage == null && fileStorage == null)
                return false;

            EncryptionSettings encryptionSettings = null;

            if (encryption != null)
            {
                encryptionSettings = new EncryptionSettings(encryption.EncryptionKey, encryption.SymmetricAlgorithmType,
                    encryption.EncryptIndexes, encryption.PreferedEncryptionKeyBitsSize);
            }

            if (isRavenFs)
            {
                var filesOrderedPartCollection = new OrderedPartCollection<Database.FileSystem.Plugins.AbstractFileCodec>();

                if (encryptionSettings != null)
                {
                    var fileEncryption = new FileEncryption();
                    fileEncryption.SetSettings(encryptionSettings);
                    filesOrderedPartCollection.Add(fileEncryption);
                }

                fileStorage.Initialize(new UuidGenerator(), filesOrderedPartCollection);
                return true;
            }
            var orderedPartCollection = new OrderedPartCollection<AbstractDocumentCodec>();

            if (encryptionSettings != null)
            {
                var documentEncryption = new DocumentEncryption();
                documentEncryption.SetSettings(encryptionSettings);
                orderedPartCollection.Add(documentEncryption);
            }

            if (hasCompression)
            {
                orderedPartCollection.Add(new DocumentCompression());
            }                                       
            storage.Initialize(new SequentialUuidGenerator {EtagBase = 0}, orderedPartCollection);
            
            return true;
        }

        public static bool ValidateStorageExists(string dataDir)
        {
            return File.Exists(Path.Combine(dataDir, Voron.Impl.Constants.DatabaseFilename))
                   || File.Exists(Path.Combine(dataDir, "Data.ravenfs")) 
                   || File.Exists(Path.Combine(dataDir, "Data"));
        }

        private static readonly string indexDefinitionFolder = "IndexDefinitions";
        private readonly string baseDirectory;
        private readonly string outputDirectory;
        private ITransactionalStorage storage;
        private Database.FileSystem.Storage.ITransactionalStorage fileStorage;
        private readonly int batchSize;
        

        public void Dispose()
        {
            if (storage != null)
                storage.Dispose();
        }
    }
}
