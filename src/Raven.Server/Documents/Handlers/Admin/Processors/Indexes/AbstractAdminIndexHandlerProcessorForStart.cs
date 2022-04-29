using System;
using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Indexes;

internal abstract class AbstractAdminIndexHandlerProcessorForStart<TRequestHandler, TOperationContext> : AbstractHandlerProxyReadProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractAdminIndexHandlerProcessorForStart([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override RavenCommand CreateCommandForNode(string nodeTag)
    {
        (string type, string name) = GetParameters();

        if (type == null && name == null)
            return new StartIndexingOperation.StartIndexingCommand(nodeTag);

        if (type != null)
            return new StartIndexingOperation.StartIndexingCommand(type, nodeTag);

        return new StartIndexOperation.StartIndexCommand(name, nodeTag);
    }

    protected (string Type, string Name) GetParameters()
    {
        var types = GetTypes();
        var names = GetNames();

        if (types.Count != 0 && names.Count != 0)
            throw new ArgumentException("Query string value 'type' and 'names' are mutually exclusive.");

        if (types.Count != 0)
        {
            if (types.Count != 1)
                throw new ArgumentException("Query string value 'type' must appear exactly once");
            if (string.IsNullOrWhiteSpace(types[0]))
                throw new ArgumentException("Query string value 'type' must have a non empty value");

            return (types[0], null);
        }

        if (names.Count != 0)
        {
            if (names.Count != 1)
                throw new ArgumentException("Query string value 'name' must appear exactly once");
            if (string.IsNullOrWhiteSpace(names[0]))
                throw new ArgumentException("Query string value 'name' must have a non empty value");

            return (null, names[0]);
        }

        return (null, null);
    }

    private StringValues GetTypes()
    {
        return RequestHandler.GetStringValuesQueryString("type", required: false);
    }

    private StringValues GetNames()
    {
        return RequestHandler.GetStringValuesQueryString("name", required: false);
    }
}
