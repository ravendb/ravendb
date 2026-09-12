using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Handlers.Processors;
using Raven.Server.Documents.TasksErrors;

namespace Raven.Server.Documents.CdcSink.Handlers.Processors;

internal sealed class CdcSinkHandlerProcessorForGetErrors : AbstractDatabaseTaskErrorsHandlerProcessorForGetErrors
{
    public CdcSinkHandlerProcessorForGetErrors([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override TaskCategory TaskCategory => TaskCategory.CdcSink;

    protected override string EndpointDebugName => "cdc-sink/errors";

    // CDC sinks are not ETL processes, so there is no live process to enrich results with.
    protected override IReadOnlyDictionary<string, EtlProcess> GetProcessesByName() => null;
}
