using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.ETL
{
    /// <summary>
    /// Operation to update an existing ETL task in the database.
    /// </summary>
    /// <typeparam name="T">The type of the connection string used for the ETL configuration.</typeparam>
    public sealed class UpdateEtlOperation<T> : IMaintenanceOperation<UpdateEtlOperationResult> where T : ConnectionString
    {
        private readonly long _taskId;
        private readonly EtlConfiguration<T> _configuration;
        private readonly List<string> _transformationsToReset;

        /// <inheritdoc cref="UpdateEtlOperation{T}"/>
        /// <param name="taskId">The identifier of the ETL task to update.</param>
        /// <param name="configuration">The new ETL configuration to apply.</param>
        /// <param name="transformationsToReset">Names of transformations to reset and reprocess all documents.</param>
        public UpdateEtlOperation(long taskId, EtlConfiguration<T> configuration, List<string> transformationsToReset = null)
        {
            _taskId = taskId;
            _configuration = configuration;
            _transformationsToReset = transformationsToReset;
        }

        public RavenCommand<UpdateEtlOperationResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx)
        {
            return new UpdateEtlCommand(conventions, _taskId, _configuration, _transformationsToReset);
        }

        internal class UpdateEtlCommand : RavenCommand<UpdateEtlOperationResult>, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly long _taskId;
            private readonly EtlConfiguration<T> _configuration;
            private readonly List<string> _transformationsToReset;

            public UpdateEtlCommand(DocumentConventions conventions, long taskId, EtlConfiguration<T> configuration, List<string> transformationsToReset)
            {
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));
                _taskId = taskId;
                _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
                _transformationsToReset = transformationsToReset;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/etl?id={_taskId}";
                
                if (_transformationsToReset != null)
                    url += $"&reset={string.Join("&reset=", _transformationsToReset)}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Put,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_configuration, ctx)).ConfigureAwait(false), _conventions)
                };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.UpdateEtlOperationResult(response);
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }

    public sealed class UpdateEtlOperationResult
    {
        public long RaftCommandIndex { get; set; }

        public long TaskId { get; set; }
    }
}
