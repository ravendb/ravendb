using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using CsvHelper;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Json;
using Raven.Client.Util;
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
        private (string, string)[] _properties;
        private bool writeHeader = true;
        
        public StreamCsvDocumentQueryResultWriter(HttpResponse response, Stream stream, DocumentsOperationContext context, string[] properties = null, string csvFileName = "export")
        {
            csvFileName = $"{csvFileName}_{SystemTime.UtcNow.ToString("yyyyMMdd_HHmm", CultureInfo.InvariantCulture)}.csv";  
           
            _response = response;
            _response.Headers["Content-Disposition"] = $"attachment; filename=\"{csvFileName}\"; filename*=UTF-8''{csvFileName}";
            
            _writer = new StreamWriter(stream, Encoding.UTF8);
            _csvWriter = new CsvWriter(_writer);
            _context = context;
            //We need to write headers without the escaping but the path should be escaped
            //so @metadata.@collection should not be written in the header as @metadata\.@collection
            //We can't escape while constructing the path since we will write the escaping in the header this way, we need both.
            _properties = properties?.Select(p => (p, Escape(p))).ToArray();
        }
        
        private char[] splitter = {'.'};
        private string Escape(string s)
        {
            var tokens = s.Split(splitter,StringSplitOptions.RemoveEmptyEntries);
            return Join('.', tokens.Select(BlittablePath.EscapeString));
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
            foreach ((var property, var path) in _properties)
            {
                if (Constants.Documents.Metadata.Id == property)
                {
                    _csvWriter.WriteField(res.Id);
                }
                else
                {
                    var o = new BlittablePath(path).Evaluate(res.Data, false);
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
                _properties = GetPropertiesRecursive((Empty, Empty), res.Data).ToArray();

            }
            writeHeader = false;
            foreach ((var property, var path) in _properties)
            {
                _csvWriter.WriteField(property);
            }

            _csvWriter.NextRecord();
        }

        private IEnumerable<(string Property, string Path)> GetPropertiesRecursive((string ParentProperty, string ParentPath) propertyTuple, BlittableJsonReaderObject obj, bool addId = true)
        {
            var inMetadata = Constants.Documents.Metadata.Key.Equals(propertyTuple.ParentPath);
            if (addId)
            {
                yield return (Constants.Documents.Metadata.Id, Constants.Documents.Metadata.Id);
            }
            foreach (var propery in obj.GetPropertyNames())
            {
                //skip properties starting with '@' unless we are in the metadata and we need to export @metadata.@collection
                if (inMetadata && propery.Equals(Constants.Documents.Metadata.Collection) == false)
                    continue;                
                if(propery.StartsWith('@') && propery.Equals(Constants.Documents.Metadata.Key) == false && propertyTuple.ParentPath.Equals(Constants.Documents.Metadata.Key) == false)
                    continue; 
                var path = IsNullOrEmpty(propertyTuple.ParentPath) ? BlittablePath.EscapeString(propery) : $"{propertyTuple.ParentPath}.{BlittablePath.EscapeString(propery)}";
                var property = IsNullOrEmpty(propertyTuple.ParentPath) ? propery : $"{propertyTuple.ParentPath}.{propery}";
                object res;
                if (obj.TryGetMember(propery, out res) && res is BlittableJsonReaderObject)
                {
                    foreach (var nested in GetPropertiesRecursive((property, path), res as BlittableJsonReaderObject, addId:false))
                    {
                        yield return nested;
                    }
                }
                else
                {                    
                    yield return (property, path);
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
