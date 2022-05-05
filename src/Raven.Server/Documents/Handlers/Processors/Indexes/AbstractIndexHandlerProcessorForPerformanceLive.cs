using System;
using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForPerformanceLive<TRequestHandler, TOperationContext> : AbstractHandlerWebSocketProxyProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    private const string IncludeSideBySideQueryString = "includeSideBySide";

    private const string NameQueryString = "name";

    protected AbstractIndexHandlerProcessorForPerformanceLive([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override string GetRemoteEndpointUrl(string databaseName)
    {
        var url = $"/databases/{databaseName}/indexes/performance/live?{IncludeSideBySideQueryString}={IncludeSideBySide}";

        var names = GetNames();
        if (names.Count > 0)
        {
            foreach (var name in names)
                url += $"&{NameQueryString}={Uri.EscapeDataString(name)}";
        }

        return url;
    }

    protected bool IncludeSideBySide => RequestHandler.GetBoolValueQueryString(IncludeSideBySideQueryString, false) ?? false;

    protected StringValues GetNames() => RequestHandler.GetStringValuesQueryString(NameQueryString, required: false);
}
