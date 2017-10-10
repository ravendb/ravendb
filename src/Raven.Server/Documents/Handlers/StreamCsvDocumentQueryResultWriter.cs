using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using CsvHelper;
using Lucene.Net.Index;
using Microsoft.AspNetCore.Http;
using Org.BouncyCastle.Utilities.Collections;
using Raven.Client.Json;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Documents.Fields;
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

        private const string _id = "@id";
        public void AddResult(Document res)
        {            
            WriteCsvHeaderIfNeeded(res);
            foreach (var property in _properties)
            {
                if (property.Length == 3 && property.Equals(_id))
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
            foreach (var propery in obj.GetPropertyNames())
            {
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
