using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Database.Util;

namespace Raven.StorgaeExporter
{
    public class StorageExporter
    {
        public StorageExporter(string databaseBaseDirectory, string databaseOutputFile,int? batchSize = null)
        {
            baseDirectory = databaseBaseDirectory;
            outputDirectory = databaseOutputFile;
            var ravenConfiguration = new InMemoryRavenConfiguration().Initialize();
            //initialize seems to override this value
            ravenConfiguration.DataDirectory = databaseBaseDirectory;
            ravenConfiguration.Storage.PreventSchemaUpdate = true;
            CreateTransactionalStorage(ravenConfiguration);
            BatchSize = batchSize ?? 1024;
        }

        public void ExportDatabase()
        {
            var stream = File.Create(outputDirectory);
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
                //documents
                jsonWriter.WritePropertyName("Docs");
                jsonWriter.WriteStartArray();
                WriteDocuments(jsonWriter);
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

        private void ReportProgress(string stage,long from, long outof)
        {
            if (from == outof) Program.ConsoleWriteLineWithColor(ConsoleColor.Green, "Completed exporting {0} out of {1} {2}",from,outof,stage);
            else Console.WriteLine("exporting {0} out of {1} {2}", from, outof, stage);
        }

        private void WriteDocuments(JsonTextWriter jsonWriter)
        {
            long totalDococCount = 0;
            long currentDocsCount = 0;
            Etag currLastEtag = Etag.Empty;
            storage.Batch(accsesor => totalDococCount = accsesor.Documents.GetDocumentsCount());
            try
            {
                CancellationToken ct = new CancellationToken();
                do
                {

                    storage.Batch(accsesor =>
                    {
                        var docs = accsesor.Documents.GetDocumentsAfter(currLastEtag, BatchSize, ct);
                        foreach (var doc in docs)
                        {
                            doc.ToJson(true).WriteTo(jsonWriter);
                        }
                        currLastEtag = docs.Last().Etag;
                        currentDocsCount += docs.Count();
                        ReportProgress("documents", currentDocsCount, totalDococCount);
                    });
                } while (totalDococCount > currentDocsCount);
            }
            catch (Exception e)
            {
                PrintErrorAndFail("Failed to export documents, error:" + e.Message);
            }
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
            int currentIdentitiesCount = 0;
            do
            {
                storage.Batch(accsesor =>
            {
                var identities = accsesor.General.GetIdentities(currentIdentitiesCount, BatchSize, out totalIdentities).Where(x=>FilterIdentity(x.Key));
                foreach (var identityInfo in identities)
                {
                    new RavenJObject
						{
							{ "Key", identityInfo.Key }, 
							{ "Value", identityInfo.Value }
						}.WriteTo(jsonWriter);
                }
                currentIdentitiesCount += identities.Count();
                ReportProgress("identities", currentIdentitiesCount, totalIdentities);
            });
            } while (totalIdentities > currentIdentitiesCount);
        }

        public bool FilterIdentity(string indentityName)
        {
            if ("Raven/Etag".Equals(indentityName, StringComparison.InvariantCultureIgnoreCase))
                return false;

            if ("IndexId".Equals(indentityName, StringComparison.InvariantCultureIgnoreCase))
                return false;
            return true;
        }

        private void CreateTransactionalStorage(InMemoryRavenConfiguration ravenConfiguration)
        {
            if (String.IsNullOrEmpty(ravenConfiguration.DataDirectory) == false && Directory.Exists(ravenConfiguration.DataDirectory))
            {
                if (File.Exists(Path.Combine(ravenConfiguration.DataDirectory, Voron.Impl.Constants.DatabaseFilename)))
                    storage = ravenConfiguration.CreateTransactionalStorage(InMemoryRavenConfiguration.VoronTypeName, () => { }, () => { });
                else if (File.Exists(Path.Combine(ravenConfiguration.DataDirectory, "Data")))
                    storage = ravenConfiguration.CreateTransactionalStorage(InMemoryRavenConfiguration.EsentTypeName, () => { }, () => { });                
                if(storage != null)
                    try
                    {
                        storage.Initialize(new SequentialUuidGenerator {EtagBase = 0}, new OrderedPartCollection<AbstractDocumentCodec>());
                    }
                    catch (UnauthorizedAccessException uae)
                    {
                        PrintErrorAndFail(String.Format("Failed to initialize the storage it is probably been locked by RavenDB.\nError message:\n{0}", uae.Message),uae.StackTrace);
                    }
                    catch (InvalidOperationException ioe)
                    {
                        PrintErrorAndFail(String.Format("Failed to initialize the storage it is probably been locked by RavenDB.\nError message:\n{0}", ioe.Message),ioe.StackTrace);
                    }
                    catch (Exception e)
                    {
                        PrintErrorAndFail(e.Message,e.StackTrace);
                        return;
                    }
                return;
            }
            PrintErrorAndFail(string.Format("Could not detect storage file under the given directory:{0}", ravenConfiguration.DataDirectory));
        }

        private void PrintErrorAndFail(string ErrorMessage,string stackTrace = null)
        {
            Console.Clear();
            Program.ConsoleWriteLineWithColor(ConsoleColor.Red, ErrorMessage);
            if (stackTrace != null)
            {
                Program.ConsoleWriteLineWithColor(ConsoleColor.Blue, "StackTrace:\n"+stackTrace);
            }
            Console.Read();
            throw new Exception(ErrorMessage);
        }

        private static readonly string indexDefinitionFolder = "IndexDefinitions";
        private string baseDirectory;
        private string outputDirectory;
        private ITransactionalStorage storage;
        private readonly int BatchSize;
    }
}
