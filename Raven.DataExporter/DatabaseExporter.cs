using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.DataExporter
{
    public class DatabaseExporter
    {
        public DatabaseExporter(string databaseBaseDirectory, string databaseOutputFile, bool includeAttachments)
        {
            baseDirectory = databaseBaseDirectory;
            outputDorectory = databaseOutputFile;
            this.includeAttachments = includeAttachments;
        }

        public void Export()
        {
            var stream = File.Create(outputDorectory);
            using (var gZipStream = new GZipStream(stream, CompressionMode.Compress,leaveOpen: true))
            using (var streamWriter = new StreamWriter(gZipStream))
            using(var exporter = new TableExporter(baseDirectory))
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
                WriteDocuments(jsonWriter, exporter);
                jsonWriter.WriteEndArray();
                // optional attachments
                jsonWriter.WritePropertyName("Attachments");
                jsonWriter.WriteStartArray();
                if (includeAttachments)
                    WriteAttachments(jsonWriter, exporter);
                jsonWriter.WriteEndArray();
                //Transformers
                jsonWriter.WritePropertyName("Transformers");
                jsonWriter.WriteStartArray();
                WriteTransformers(jsonWriter);
                jsonWriter.WriteEndArray();
                //Identities
                jsonWriter.WritePropertyName("Identities");
                jsonWriter.WriteStartArray();
                WriteIdentities(jsonWriter, exporter);
                jsonWriter.WriteEndArray();
                //end of export
                jsonWriter.WriteEndObject();
                streamWriter.Flush();
            }
        }

        private void WriteDocuments(JsonTextWriter jsonWriter, TableExporter exporter)
        {
            exporter.ExportDocuments(jsonWriter);
            
        }

        private void WriteTransformers(JsonTextWriter jsonWriter)
        {
            var indexDefinitionsBasePath = Path.Combine(baseDirectory, indexDefinitionFolder);
            foreach (var file in Directory.GetFiles(indexDefinitionsBasePath, "*.transform"))
            {
                var ravenObj = RavenJObject.Parse(File.ReadAllText(file));
                jsonWriter.WriteStartObject();
                jsonWriter.WritePropertyName("name");
                jsonWriter.WriteValue(ravenObj.Value<string>("Name"));
                jsonWriter.WritePropertyName("definition");
                ravenObj.WriteTo(jsonWriter);
                jsonWriter.WriteEndObject();

            }
        }
        private void WriteIndexes(JsonTextWriter jsonWriter)
        {
            var indexDefinitionsBasePath = Path.Combine(baseDirectory, indexDefinitionFolder);
            foreach (var file in Directory.GetFiles(indexDefinitionsBasePath, "*.index"))
            {
                var ravenObj = RavenJObject.Parse(File.ReadAllText(file));
                jsonWriter.WriteStartObject();
                jsonWriter.WritePropertyName("name");
                jsonWriter.WriteValue(ravenObj.Value<string>("Name"));
                jsonWriter.WritePropertyName("definition");
                ravenObj.WriteTo(jsonWriter);
                jsonWriter.WriteEndObject();
                
            }
        }

        private void WriteAttachments(JsonTextWriter jsonWriter, TableExporter exporter)
        {
            exporter.ExportAttachments(jsonWriter);
          
        }

        private void WriteIdentities(JsonTextWriter jsonWriter, TableExporter exporter)
        {
            exporter.ExportIdentities(jsonWriter);  
        }

        private static readonly string indexDefinitionFolder = "IndexDefinitions";
        private string baseDirectory;
        private string outputDorectory;
        private bool includeAttachments;
    }
}
