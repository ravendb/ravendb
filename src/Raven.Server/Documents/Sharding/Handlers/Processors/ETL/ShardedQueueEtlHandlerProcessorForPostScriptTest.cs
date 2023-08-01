using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.ETL;
using Raven.Server.Documents.ETL.Providers.Queue.Test;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.ETL
{
    internal sealed class ShardedQueueEtlHandlerProcessorForPostScriptTest : AbstractShardedEtlHandlerProcessorForTest<TestQueueEtlScript, QueueEtlConfiguration, QueueConnectionString>
    {
        public ShardedQueueEtlHandlerProcessorForPostScriptTest([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
            throw new NotSupportedInShardingException("Queue ETLs are currently not supported in sharding");
        }

        protected override TestQueueEtlScript GetTestEtlScript(BlittableJsonReaderObject json) => JsonDeserializationServer.TestQueueEtlScript(json);

        protected override RavenCommand CreateCommand(BlittableJsonReaderObject json) => new QueueEtlTestCommand(RequestHandler.ShardExecutor.Conventions, json);
    }
}
