using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    /// <summary>
    /// Operation to retrieve a list of unique terms from a specific index field.
    /// </summary>
    public sealed class GetTermsOperation : IMaintenanceOperation<string[]>
    {
        private readonly string _indexName;
        private readonly string _field;
        private readonly string _fromValue;
        private readonly int? _pageSize;

        /// <inheritdoc cref="GetTermsOperation"/>
        /// <param name="indexName">The name of the index from which to retrieve terms.</param>
        /// <param name="field">The field within the index to retrieve terms from.</param>
        /// <param name="fromValue">The starting term value from which to begin retrieval.</param>
        /// <param name="pageSize">The maximum number of terms to return (optional).</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="indexName"/> or <paramref name="field"/> is null.</exception>
        public GetTermsOperation(string indexName, string field, string fromValue, int? pageSize = null)
        {
            _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
            _field = field ?? throw new ArgumentNullException(nameof(field));
            _fromValue = fromValue;
            _pageSize = pageSize;
        }

        public RavenCommand<string[]> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetTermsCommand(_indexName, _field, _fromValue, _pageSize);
        }

        internal sealed class GetTermsCommand : RavenCommand<string[]>
        {
            private readonly string _indexName;
            private readonly string _field;
            private readonly string _fromValue;
            private readonly int? _pageSize;

            public GetTermsCommand(string indexName, string field, string fromValue, int? pageSize)
            {
                _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
                _field = field ?? throw new ArgumentNullException(nameof(field));
                _fromValue = fromValue;
                _pageSize = pageSize;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var fromValue = _fromValue != null ? Uri.EscapeDataString(_fromValue) : ""; 
                url = $"{node.Url}/databases/{node.Database}/indexes/terms?name={Uri.EscapeDataString(_indexName)}&field={Uri.EscapeDataString(_field)}&fromValue={fromValue}&pageSize={_pageSize}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                var result = JsonDeserializationClient.TermsQueryResult(response);
                var terms = result.Terms;

                Result = terms.ToArray();
            }

            public override bool IsReadRequest => true;
        }
    }
}
