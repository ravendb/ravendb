using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.Queue.Handlers.Processors;

internal sealed class
    QueueEtlHandlerProcessorForTestAzureServiceBusConnection<TRequestHandler, TOperationContext> :
    AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    public QueueEtlHandlerProcessorForTestAzureServiceBusConnection([NotNull] TRequestHandler requestHandler) :
        base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using var token = RequestHandler.CreateHttpRequestBoundTimeLimitedOperationToken(TimeSpan.FromSeconds(30));
        using var _ = ContextPool.AllocateOperationContext(out JsonOperationContext context);
        var authenticationJson = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "azure-service-bus", token.Token);
        var connectionSettings = JsonDeserializationServer.AzureServiceBusConnectionSettings(authenticationJson);
        var result = new DynamicJsonValue();

        try
        {
            var probeEntity = $"ravendb-connection-test-{Guid.NewGuid():N}";
            await using var client = QueueBrokerConnectionHelper.CreateAzureServiceBusClient("RavenDB-test-connectivity", connectionSettings);
            await using var receiver = client.CreateReceiver(probeEntity);
            await CheckConnectivityAsync(receiver, token);
            result[nameof(NodeConnectionTestResult.Success)] = true;
        }
        catch (Exception ex)
        {
            result[nameof(NodeConnectionTestResult.Success)] = false;
            result[nameof(NodeConnectionTestResult.Error)] = ex.ToString();
        }

        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream(), token.Token))
        {
            context.Write(writer, result);
        }
    }

    private static async ValueTask CheckConnectivityAsync(ServiceBusReceiver receiver, OperationCancelToken token)
    {
        try
        {
            await receiver.PeekMessageAsync(cancellationToken: token.Token);
        }
        catch (ServiceBusException ex) when (ex.Reason == ServiceBusFailureReason.MessagingEntityNotFound)
        {
            // Credentials and network are valid; the probe entity not existing is expected.
        }
    }
}
