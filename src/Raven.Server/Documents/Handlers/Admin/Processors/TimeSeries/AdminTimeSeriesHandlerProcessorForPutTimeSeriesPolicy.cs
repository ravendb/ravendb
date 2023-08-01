using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.TimeSeries
{
    internal sealed class AdminTimeSeriesHandlerProcessorForPutTimeSeriesPolicy : AbstractAdminTimeSeriesHandlerProcessorForPutTimeSeriesPolicy<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AdminTimeSeriesHandlerProcessorForPutTimeSeriesPolicy([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
