using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Admin.Processors.TimeSeries
{
    internal sealed class AdminTimeSeriesHandlerProcessorForPutTimeSeriesPolicy : AbstractAdminTimeSeriesHandlerProcessorForPutTimeSeriesPolicy<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AdminTimeSeriesHandlerProcessorForPutTimeSeriesPolicy([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
