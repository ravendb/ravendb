using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.TimeSeries
{
    internal sealed class AdminTimeSeriesHandlerProcessorForDeleteTimeSeriesPolicy : AbstractAdminTimeSeriesHandlerProcessorForDeleteTimeSeriesPolicy<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AdminTimeSeriesHandlerProcessorForDeleteTimeSeriesPolicy([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
