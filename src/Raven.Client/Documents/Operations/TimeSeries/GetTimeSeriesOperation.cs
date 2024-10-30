using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session.Loaders;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.TimeSeries
{
    public sealed class GetTimeSeriesOperation : GetTimeSeriesOperation<TimeSeriesEntry>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="GetTimeSeriesOperation"/> class, 
        /// retrieving multiple entries from a time series associated with a document within a specified date range.
        /// </summary>
        /// <inheritdoc select="param" />
        public GetTimeSeriesOperation(string docId, string timeseries, DateTime? @from = null, DateTime? to = null, int start = 0, int pageSize = int.MaxValue, bool returnFullResults = false) : base(docId, timeseries, @from, to, start, pageSize, returnFullResults)
        {
        }
    }


    public class GetTimeSeriesOperation<TValues> : IOperation<TimeSeriesRangeResult<TValues>> where TValues : TimeSeriesEntry
    {
        private readonly string _docId, _name;
        private readonly int _start, _pageSize;
        private readonly DateTime? _from, _to;
        private readonly Action<ITimeSeriesIncludeBuilder> _includes;
        private readonly bool _returnFullResults;

        /// <summary>
        /// Initializes a new instance of the <see cref="GetTimeSeriesOperation{TValues}"/> class, 
        /// retrieving multiple entries from a time series associated with a document within a specified date range.
        /// </summary>
        /// <typeparam name="TValues">
        /// The type to which each time series entry should be projected. 
        /// Must be a subtype of <see cref="TimeSeriesEntry"/>.
        /// </typeparam>
        /// <param name="docId">The ID of the document that holds the time series.</param>
        /// <param name="timeseries">The name of the time series to retrieve.</param>
        /// <param name="from">The start date of the range from which entries should be retrieved. If <c>null</c>, retrieval begins from the earliest entry.</param>
        /// <param name="to">The end date of the range up to which entries should be retrieved. If <c>null</c>, retrieval continues to the latest entry.</param>
        /// <param name="start">The start index for pagination of results.</param>
        /// <param name="pageSize">The number of entries to retrieve. Defaults to <c>int.MaxValue</c> for retrieving all entries within the specified range.</param>
        /// <param name="returnFullResults">Whether to include detailed information for each entry. If <c>false</c>, retrieves only basic information.</param>
        public GetTimeSeriesOperation(string docId, string timeseries, DateTime? from = null, DateTime? to = null, int start = 0, int pageSize = int.MaxValue, bool returnFullResults = false) 
            : this(docId, timeseries, from, to, start, pageSize, includes: null, returnFullResults)
        { }

        internal GetTimeSeriesOperation(string docId, string timeseries, DateTime? from, DateTime? to, int start, int pageSize, Action<ITimeSeriesIncludeBuilder> includes, bool returnFullResults = false)
        {
            if (string.IsNullOrEmpty(docId))
                throw new ArgumentNullException(nameof(docId));

            if (string.IsNullOrEmpty(timeseries))
                throw new ArgumentNullException(nameof(timeseries));

            _docId = docId;
            _start = start;
            _pageSize = pageSize;
            _name = timeseries;
            _from = from;
            _to = to;
            _includes = includes;
            _returnFullResults = returnFullResults;
        }


        public RavenCommand<TimeSeriesRangeResult<TValues>> GetCommand(IDocumentStore store, DocumentConventions conventions, JsonOperationContext context,
            HttpCache cache)
        {
            return new GetTimeSeriesCommand(_docId, _name, _from, _to, _start, _pageSize, _includes, _returnFullResults);
        }

        internal class GetTimeSeriesCommand : RavenCommand<TimeSeriesRangeResult<TValues>>
        {
            private readonly string _docId, _name;
            private readonly int _start, _pageSize;
            private readonly DateTime? _from, _to;
            private readonly Action<ITimeSeriesIncludeBuilder> _includes;
            private readonly bool _returnFullResults;

            public GetTimeSeriesCommand(string docId, string name, DateTime? @from, DateTime? to, int start, int pageSize, Action<ITimeSeriesIncludeBuilder> includes, bool returnFullResults = false)
            {
                _docId = docId;
                _name = name;
                _start = start;
                _pageSize = pageSize;
                _from = from;
                _to = to;
                _includes = includes;
                _returnFullResults = returnFullResults;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var pathBuilder = new StringBuilder(node.Url);
                pathBuilder.Append("/databases/")
                    .Append(node.Database)
                    .Append("/timeseries")
                    .Append("?docId=")
                    .Append(Uri.EscapeDataString(_docId));

                if (_start > 0)
                {
                    pathBuilder.Append("&start=")
                        .Append(_start);
                }

                if (_pageSize < int.MaxValue)
                {
                    pathBuilder.Append("&pageSize=")
                        .Append(_pageSize);
                }

                pathBuilder.Append("&name=")
                    .Append(Uri.EscapeDataString(_name));

                if (_from.HasValue)
                {
                    pathBuilder.Append("&from=")
                        .Append(_from.Value.EnsureUtc().GetDefaultRavenFormat());
                }

                if (_to.HasValue)
                {
                    pathBuilder.Append("&to=")
                        .Append(_to.Value.EnsureUtc().GetDefaultRavenFormat());
                }

                if (_includes != null)
                {
                    AddIncludesToRequest(pathBuilder, _includes);
                }

                if (_returnFullResults)
                {
                    pathBuilder.Append("&full=").Append(true);
                }

                url = pathBuilder.ToString();

                var request = new HttpRequestMessage { Method = HttpMethod.Get };

                return request;
            }

            internal static void AddIncludesToRequest(StringBuilder pathBuilder, Action<ITimeSeriesIncludeBuilder> includes)
            {
                var includeBuilder = new IncludeBuilder<TValues>(DocumentConventions.Default);
                includes.Invoke(includeBuilder);

                if (includeBuilder.IncludeTimeSeriesDocument)
                {
                    pathBuilder.Append("&includeDocument=true");
                }

                if (includeBuilder.IncludeTimeSeriesTags)
                {
                    pathBuilder.Append("&includeTags=true");
                }
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                if (JsonDeserializationClient.CacheForTimeSeriesRangeResult.TryGetValue(typeof(TValues), out var func) == false)
                {
                    func = JsonDeserializationBase.GenerateJsonDeserializationRoutine<TimeSeriesRangeResult<TValues>>();
                    JsonDeserializationClient.CacheForTimeSeriesRangeResult.TryAdd(typeof(TValues), func);
                }

                Result = (TimeSeriesRangeResult<TValues>)func(response);
            }

            public override bool IsReadRequest => true;
        }
    }
}
