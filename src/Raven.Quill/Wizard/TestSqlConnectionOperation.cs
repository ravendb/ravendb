using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Quill.Wizard;

internal sealed class TestSqlConnectionOperation : IMaintenanceOperation<ConnectResult>
{
    private readonly string _factoryName;
    private readonly string _connectionString;

    public TestSqlConnectionOperation(string factoryName, string connectionString)
    {
        _factoryName = factoryName ?? throw new ArgumentNullException(nameof(factoryName));
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
    }

    public RavenCommand<ConnectResult> GetCommand(DocumentConventions conventions, JsonOperationContext ctx) =>
        new TestConnectionCommand(_factoryName, _connectionString);

    private sealed class TestConnectionCommand(string factoryName, string connectionString)
        : RavenCommand<ConnectResult>
    {
        public override bool IsReadRequest => true;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/etl/sql/test-connection?factoryName={Uri.EscapeDataString(factoryName)}";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = new StringContent(connectionString),
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
            {
                ThrowInvalidResponse();
                return;
            }

            var result = new ConnectResult();

            if (response.TryGet("Success", out bool success))
                result.Success = success;

            if (response.TryGet("Error", out string? error) && string.IsNullOrEmpty(error) == false)
                result.Errors.Add(error);

            Result = result;
        }
    }
}
