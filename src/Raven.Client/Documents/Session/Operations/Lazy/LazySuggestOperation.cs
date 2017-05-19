using System;
using System.Globalization;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Suggestion;

namespace Raven.Client.Documents.Session.Operations.Lazy
{
    internal class LazySuggestOperation : ILazyOperation
    {
        private readonly string _index;
        private readonly SuggestionQuery _suggestionQuery;

        public LazySuggestOperation(string index, SuggestionQuery suggestionQuery)
        {
            _index = index;
            _suggestionQuery = suggestionQuery;
        }

        public GetRequest CreateRequest()
        {
            var query = string.Format(
                "term={0}&field={1}&max={2}",
                _suggestionQuery.Term,
                _suggestionQuery.Field,
                _suggestionQuery.MaxSuggestions);

            if (_suggestionQuery.Accuracy.HasValue)
                query += "&accuracy=" + _suggestionQuery.Accuracy.Value.ToString(CultureInfo.InvariantCulture);

            if (_suggestionQuery.Distance.HasValue)
                query += "&distance=" + _suggestionQuery.Distance;

            return new GetRequest
            {
                Url = "/suggest/" + _index,
                Query = query
            };
        }

        public object Result { get; private set; }
        public QueryResult QueryResult { get; set; }
        public bool RequiresRetry { get; private set; }
        public void HandleResponse(GetResponse response)
        {
            throw new NotImplementedException("This feature is not yet implemented");

            /*if (response.Status != 200 && response.Status != 304)
            {
                throw new InvalidOperationException("Got an unexpected response code for the request: " + response.Status + "\r\n" +
                                                    response.Result);
            }

            var result = (RavenJObject)response.Result;
            Result = new SuggestionQueryResult
            {
                Suggestions = ((RavenJArray)result["Suggestions"]).Select(x => x.Value<string>()).ToArray(),
            };*/
        }

        public IDisposable EnterContext()
        {
            return null;
        }
    }
}
