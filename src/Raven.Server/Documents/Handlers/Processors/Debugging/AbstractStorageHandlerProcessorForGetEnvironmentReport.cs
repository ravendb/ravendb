using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Storage;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Impl;

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

    protected virtual DynamicJsonValue GetJsonReport(StorageEnvironmentWithType env, LowLevelTransaction lowTx, bool de)
    {
        throw new NotSupportedException();
    }

    public void WriteEnvironmentsReport(AsyncBlittableJsonTextWriter writer, string name, IEnumerable<StorageEnvironmentWithType> envs,
        DocumentsOperationContext context, bool detailed = false)
    {
        bool first = true;

        writer.WriteStartObject();

        writer.WritePropertyName("DatabaseName");
        writer.WriteString(name);
        writer.WriteComma();

        writer.WritePropertyName("Environments");
        writer.WriteStartArray();
        foreach (var env in envs)
        {
            if (env == null)
                continue;

            if (!first)
                writer.WriteComma();
            first = false;

            WriteReport(writer, env, context, detailed);
        }

        writer.WriteEndArray();

        writer.WriteEndObject();
    }

    public void WriteReport(AsyncBlittableJsonTextWriter writer, StorageEnvironmentWithType env, DocumentsOperationContext context, bool detailed = false)
    {
        if (env == null)
            return;

        writer.WriteStartObject();

        writer.WritePropertyName("Name");
        writer.WriteString(env.Name);
        writer.WriteComma();

        writer.WritePropertyName("Type");
        writer.WriteString(env.Type.ToString());
        writer.WriteComma();

        using (var tx = context.OpenWriteTransaction())
        {
            var djv = GetJsonReport(env, tx.InnerTransaction.LowLevelTransaction, detailed);
            writer.WritePropertyName("Report");
            writer.WriteObject(context.ReadObject(djv, env.Name));
        }

        writer.WriteEndObject();
    }
}
