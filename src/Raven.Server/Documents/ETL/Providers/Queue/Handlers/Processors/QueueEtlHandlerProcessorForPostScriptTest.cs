using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Documents.ETL.Providers.Queue.Test;
using Raven.Server.Documents.ETL.Providers.Raven.Handlers.Processors;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.Queue.Handlers.Processors
{
    internal sealed class QueueEtlHandlerProcessorForPostScriptTest : AbstractDatabaseEtlHandlerProcessorForTest<TestQueueEtlScript, QueueEtlConfiguration, QueueConnectionString>
    {
        public QueueEtlHandlerProcessorForPostScriptTest([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override TestQueueEtlScript GetTestEtlScript(BlittableJsonReaderObject json) => JsonDeserializationServer.TestQueueEtlScript(json);
    }
}
