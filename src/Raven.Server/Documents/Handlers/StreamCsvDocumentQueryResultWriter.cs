using System;
using System.IO;
using System.Text;
using CsvHelper;
using Microsoft.AspNetCore.Http;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers
{
    internal class StreamCsvDocumentQueryResultWriter : IStreamDocumentQueryResultWriter
    {
        private HttpResponse _response;
        private DocumentsOperationContext _context;
        private StreamWriter _writer;
        private CsvWriter _csvWriter;
        private string[] _properties;

        public StreamCsvDocumentQueryResultWriter(HttpResponse response, Stream stream, DocumentsOperationContext context, string[] properties = null)
        {
            _response = response;
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
            if (_properties == null)
            {
                GetPropertiesAndWriteCsvHeader(res);
            }
            foreach (var property in _properties)
            {
                object r;
                //TODO: verify that all blittable property types implement ToString
                if (res.Data.TryGetMember(property, out r))
                {
                    _csvWriter.WriteField(res);
                }
                else
                {
                    _csvWriter.WriteField(null);
                }
            }
            _csvWriter.NextRecord();
        }

        private void GetPropertiesAndWriteCsvHeader(Document result)
        {
            _properties = result.Data.GetPropertyNames();
            foreach (var property in _properties)
            {
                _csvWriter.WriteField(property);
            }

            _csvWriter.NextRecord();
        }

        public void EndResponse()
        {
        }

        public void WriteError(Exception e)
        {
            throw new NotImplementedException();
        }

        public void WriteError(string error)
        {
            throw new NotImplementedException();
        }

        public void WriteQueryStatistics(long resultEtag, bool isStale, string indexName, long totalResults, DateTime timestamp)
        {
            throw new NotImplementedException();
        }

        public bool SupportError => false;
        public bool SupportStatistics => false;
    }
}
