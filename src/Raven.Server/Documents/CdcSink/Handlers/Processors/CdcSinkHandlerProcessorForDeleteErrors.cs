using Raven.Server.Documents.ETL.Handlers.Processors;
using Raven.Server.Documents.TasksErrors;

namespace Raven.Server.Documents.CdcSink.Handlers.Processors;

internal sealed class CdcSinkHandlerProcessorForDeleteErrors : AbstractDatabaseTaskErrorsHandlerProcessorForDeleteErrors
{
    public CdcSinkHandlerProcessorForDeleteErrors(DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override TaskCategory TaskCategory => TaskCategory.CdcSink;
}
