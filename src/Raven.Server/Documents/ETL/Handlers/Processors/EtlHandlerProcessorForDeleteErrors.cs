using Raven.Server.Documents.TasksErrors;

namespace Raven.Server.Documents.ETL.Handlers.Processors;

internal sealed class EtlHandlerProcessorForDeleteErrors : AbstractDatabaseTaskErrorsHandlerProcessorForDeleteErrors
{
    public EtlHandlerProcessorForDeleteErrors(DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override TaskCategory TaskCategory => TaskCategory.Etl;
}
