using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations.Counters;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Explanation;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class StreamDocumentIndexEntriesQueryResult : QueryResultServerSide<BlittableJsonReaderObject>
    {
        private readonly IStreamBlittableJsonReaderObjectQueryResultWriter _writer;
        private readonly OperationCancelToken _token;
        private bool _anyWrites;
        private bool _anyExceptions;

        public StreamDocumentIndexEntriesQueryResult(HttpResponse response, IStreamBlittableJsonReaderObjectQueryResultWriter writer, OperationCancelToken token)
        {
            if (response.HasStarted)
                throw new InvalidOperationException("You cannot start streaming because response has already started.");

            _writer = writer;
            _token = token;
        }

        public override void AddResult(BlittableJsonReaderObject result)
        {
            if (_anyWrites == false)
                StartResponseIfNeeded();

            using (result)
                _writer.AddResult(result);
            _token.Delay();
        }

        //public override void AddResult(Document result)
        //{
        //    if (_anyWrites == false)
        //        StartResponseIfNeeded();

        //    using (result.Data)
        //        _writer.AddResult(result);
        //    _token.Delay();
        //}

        public override void AddHighlightings(Dictionary<string, Dictionary<string, string[]>> highlightings)
        {
            throw new NotSupportedException();
        }

        public override void AddExplanation(ExplanationResult explanationResult)
        {
            throw new NotSupportedException();
        }

        public override void AddCounterIncludes(IncludeCountersCommand includeCountersCommand)
        {
            throw new NotSupportedException();
        }

        public override Dictionary<string, List<CounterDetail>> GetCounterIncludes()
        {
            throw new NotSupportedException();
        }

        public override void HandleException(Exception e)
        {
            StartResponseIfNeeded();

            _anyExceptions = true;

            _writer.EndResults();
            _writer.WriteError(e);

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
        public override bool SupportsHighlighting => false;
        public override bool SupportsExplanations => false;

        public void Flush()// intentionally not using Disposable here, because we need better error handling
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
