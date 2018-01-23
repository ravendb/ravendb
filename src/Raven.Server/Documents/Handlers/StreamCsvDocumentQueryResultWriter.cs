using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using CsvHelper;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Json;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using static System.String;

namespace Raven.Server.Documents.Handlers
{
    internal class StreamCsvDocumentQueryResultWriter : IStreamDocumentQueryResultWriter
    {
        private HttpResponse _response;
        private DocumentsOperationContext _context;
        private StreamWriter _writer;
        private CsvWriter _csvWriter;
        private string[] _properties;
        private const string FileName = "export.csv";
        private bool writeHeader = true;
        public StreamCsvDocumentQueryResultWriter(HttpResponse response, Stream stream, DocumentsOperationContext context, string[] properties = null)
        {
            _response = response;
            _response.Headers["Content-Disposition"] = $"attachment; filename=\"{FileName}\"; filename*=UTF-8''{FileName}";
            _writer = new StreamWriter(stream, Encoding.UTF8);
            _csvWriter = new CsvWriter(_writer);
            _context = context;
            _properties = properties;
        }

        public void Dispose()
        {
            _csvWriter?.Dispose();
            _writer?.Dispose();
        }

        public void StartResponse()
        {
        }

        public void StartResults()
        {
        }

        public void EndResults()
        {
        }

        public void AddResult(Document res)
        {            
            WriteCsvHeaderIfNeeded(res);
            foreach (var property in _properties)
            {
                if (Constants.Documents.Metadata.Id == property)
                {
                    _csvWriter.WriteField(res.Id);
                }
                else
                {
                    var o = new BlittablePath(property).Evaluate(res.Data, false);
                    _csvWriter.WriteField(o?.ToString());
                }                
            }
            _csvWriter.NextRecord();
        }

        private void WriteCsvHeaderIfNeeded(Document res)
        {
            if(writeHeader == false)
                return;
            if (_properties == null)
            {
                _properties = GetPropertiesRecursive(Empty, res.Data).ToArray();
            }
            writeHeader = false;
            foreach (var property in _properties)
            {
                _csvWriter.WriteField(property);
            }

            _csvWriter.NextRecord();
        }

        private IEnumerable<string> GetPropertiesRecursive(string parentPath, BlittableJsonReaderObject obj)
        {
            var inMetadata = Constants.Documents.Metadata.Key.Equals(parentPath);
            foreach (var propery in obj.GetPropertyNames())
            {
                //skip properties starting with '@' unless we are in the metadata and we need to export @metadata.@collection
                if (inMetadata && propery.Equals(Constants.Documents.Metadata.Collection) == false)
                    continue;                
                if(propery.StartsWith('@') && propery.Equals(Constants.Documents.Metadata.Key) == false && parentPath.Equals(Constants.Documents.Metadata.Key) == false)
                    continue; 
                var path = IsNullOrEmpty(parentPath) ? propery : $"{parentPath}.{propery}";
                object res;
                if (obj.TryGetMember(propery, out res) && res is BlittableJsonReaderObject)
                {
                    foreach (var nested in GetPropertiesRecursive(path, res as BlittableJsonReaderObject))
                    {
                        yield return nested;
                    }
                }
                else
                {                    
                    yield return path;
                }
            }
        }

        public void EndResponse()
        {
        }

        public void WriteError(Exception e)
        {
            _writer.WriteLine(e.ToString());
        }

        public void WriteError(string error)
        {
            _writer.WriteLine(error);
        }

        public void WriteQueryStatistics(long resultEtag, bool isStale, string indexName, long totalResults, DateTime timestamp)
        {
            throw new NotImplementedException();
        }

        public bool SupportError => false;
        public bool SupportStatistics => false;
    }
}
