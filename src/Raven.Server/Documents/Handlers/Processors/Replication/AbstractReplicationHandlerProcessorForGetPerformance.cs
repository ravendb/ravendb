using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Replication;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal abstract class AbstractReplicationHandlerProcessorForGetPerformance<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractReplicationHandlerProcessorForGetPerformance([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected abstract IEnumerable<IAbstractIncomingReplicationHandler> GetIncomingHandlers(TOperationContext context);

        protected abstract IEnumerable<IReportOutgoingReplicationPerformance> GetOutgoingReplicationReportsPerformance(TOperationContext context);

        public override async ValueTask ExecuteAsync()
        {
            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                var incomingHandlers = GetIncomingHandlers(context);
                var outgoingReplicationReports = GetOutgoingReplicationReportsPerformance(context);
                await WriteResultsAsync(context, incomingHandlers, outgoingReplicationReports);
            }
        }

        protected async ValueTask WriteResultsAsync(JsonOperationContext context, IEnumerable<IAbstractIncomingReplicationHandler> incomingHandlers,
            IEnumerable<IReportOutgoingReplicationPerformance> outgoingReplicationReports)
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WriteArray(context, nameof(ReplicationPerformance.Incoming), incomingHandlers, (w, c, handler) =>
                {
                    w.WriteStartObject();

                    w.WritePropertyName(nameof(ReplicationPerformance.IncomingStats.Source));
                    w.WriteString(handler.SourceFormatted);
                    w.WriteComma();

                    w.WriteArray(c, nameof(ReplicationPerformance.IncomingStats.Performance), handler.GetReplicationPerformance(), (innerWriter, innerContext, performance) =>
                    {
                        var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(performance);
                        innerWriter.WriteObject(context.ReadObject(djv, "replication/performance"));
                    });

                    w.WriteEndObject();
                });

                if (outgoingReplicationReports != null)
                {
                    writer.WriteComma();

                    writer.WriteArray(context, nameof(ReplicationPerformance.Outgoing), outgoingReplicationReports, (w, c, handler) =>
                    {
                        w.WriteStartObject();

                        w.WritePropertyName(nameof(ReplicationPerformance.OutgoingStats.Destination));
                        w.WriteString(handler.DestinationFormatted);
                        w.WriteComma();

                        w.WriteArray(c, nameof(ReplicationPerformance.OutgoingStats.Performance), handler.GetReplicationPerformance(), (innerWriter, innerContext, performance) =>
                        {
                            var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(performance);
                            innerWriter.WriteObject(context.ReadObject(djv, "replication/performance"));
                        });

                        w.WriteEndObject();
                    });
                }

                writer.WriteEndObject();
            }
        }
    }
}
