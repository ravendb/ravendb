using System;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Session;
using Raven.Server.Json;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class StreamDocumentQueryResult : QueryResultServerSide, IDisposable
    {
        private readonly IStreamDocumentQueryResultWriter _writer;
        private bool _anyWrites;
        private bool _anyExceptions;

        public StreamDocumentQueryResult(HttpResponse response, IStreamDocumentQueryResultWriter writer)
        {
            if (response.HasStarted)
                throw new InvalidOperationException("You cannot start streaming because response has already started.");

            _writer = writer;
        }

        public override void AddResult(Document result)
        {
            if (_anyWrites == false)
                StartResponseIfNeeded();

            _writer.AddResult(result);            
        }

        public override void HandleException(Exception e)
        {
            StartResponseIfNeeded();

            _anyExceptions = true;

            _writer.EndResults();
            if (_writer.SupportError)
            {
                _writer.WriteError(e);
            }            

            throw e;
        }

        private void StartResponseIfNeeded()
        {
            if (_anyWrites)
                return;

            StartResponse();

            _anyWrites = true;
        }

        public override bool SupportsExceptionHandling => true;

        public override bool SupportsInclude => false;

        public void Dispose()
        {
            StartResponseIfNeeded();

            EndResponse();
        }

        private void StartResponse()
        {
            _writer.StartResponse();            

            if (_writer.SupportStatistics)
            {
                _writer.WriteQueryStatistics(ResultEtag, IsStale, IndexName, TotalResults, IndexTimestamp);
            }
            _writer.StartResults();
            
        }

        private void EndResponse()
        {
            if (_anyExceptions == false)
                _writer.EndResults();

            _writer.EndResponse();
        }
    }
}
