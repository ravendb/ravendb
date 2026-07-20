using System.Diagnostics.CodeAnalysis;
using Raven.Server.Documents.TasksErrors;

namespace Raven.Server.Documents.ETL.Handlers.Processors;

internal sealed class EtlHandlerProcessorForGetErrors : AbstractDatabaseTaskErrorsHandlerProcessorForGetErrors
{
    public EtlHandlerProcessorForGetErrors([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override TaskCategory TaskCategory => TaskCategory.Etl;

    protected override string EndpointDebugName => "etl/errors";
}
