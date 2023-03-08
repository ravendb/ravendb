using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.ETL;
using Raven.Server.Documents.ETL.Providers.Queue.Test;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.ETL
{
    internal class ShardedQueueEtlHandlerProcessorForPostScriptTest : AbstractShardedEtlHandlerProcessorForTest<TestQueueEtlScript, QueueEtlConfiguration, QueueConnectionString>
    {
        public ShardedQueueEtlHandlerProcessorForPostScriptTest([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override TestQueueEtlScript GetTestEtlScript(BlittableJsonReaderObject json) => JsonDeserializationServer.TestQueueEtlScript(json);

        protected override RavenCommand CreateCommand(BlittableJsonReaderObject json) => new QueueEtlTestCommand(RequestHandler.ShardExecutor.Conventions, json);
    }
}
