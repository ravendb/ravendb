using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.TimeSeries
{
    internal class TimeSeriesHandlerProcessorForPostTimeSeriesNamesConfiguration : AbstractTimeSeriesHandlerProcessorForPostTimeSeriesNamesConfiguration<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public TimeSeriesHandlerProcessorForPostTimeSeriesNamesConfiguration([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
