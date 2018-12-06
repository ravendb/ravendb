using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations.Counters;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Explanation;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public abstract class StreamQueryResult<T> : QueryResultServerSide<T>
    {
        private readonly IStreamQueryResultWriter<T> _writer;
        private readonly OperationCancelToken _token;
        private bool _anyWrites;
        private bool _anyExceptions;

        protected StreamQueryResult(HttpResponse response, IStreamQueryResultWriter<T> writer, OperationCancelToken token)
        {
            if (response.HasStarted)
                throw new InvalidOperationException("You cannot start streaming because response has already started.");

            _writer = writer;
            _token = token;
        }

        public IStreamQueryResultWriter<T> GetWriter()
        {
            return _writer;
        }

        public OperationCancelToken GetToken()
        {
            return _token;
        }

        public bool GetAnyWrites()
        {
            return _anyWrites;
        }
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

        public void StartResponseIfNeeded()
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
