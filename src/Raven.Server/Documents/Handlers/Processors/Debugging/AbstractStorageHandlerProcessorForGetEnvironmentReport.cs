using System;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Storage;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.Handlers.Processors.Debugging;

internal abstract class AbstractStorageHandlerProcessorForGetEnvironmentReport<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<object, TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractStorageHandlerProcessorForGetEnvironmentReport([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override RavenCommand<object> CreateCommandForNode(string nodeTag)
    {
        var name = GetName();
        var type = GetEnvironmentType();
        var details = GetDetails();

        return new GetEnvironmentStorageReportCommand(name, type, details, nodeTag);
    }

    protected string GetName() => RequestHandler.GetStringQueryString("name");

    protected StorageEnvironmentWithType.StorageEnvironmentType GetEnvironmentType()
    {
        var typeAsString = RequestHandler.GetStringQueryString("type");

        if (Enum.TryParse(typeAsString, out StorageEnvironmentWithType.StorageEnvironmentType type) == false)
            throw new InvalidOperationException("Query string value 'type' is not a valid environment type: " + typeAsString);

        return type;
    }

    protected bool GetDetails() => RequestHandler.GetBoolValueQueryString("details", required: false) ?? false;
}
