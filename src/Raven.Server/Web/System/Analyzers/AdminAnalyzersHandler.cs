﻿using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Server.Documents.Indexes.Analysis;
using Raven.Server.Json;
using Raven.Server.Rachis;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands.Analyzers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Web.System.Analyzers
{
    public sealed class AdminAnalyzersHandler : ServerRequestHandler
    {
        [RavenAction("/admin/analyzers", "PUT", AuthorizationStatus.Operator)]
        public async Task Put()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var input = await context.ReadForMemoryAsync(RequestBodyStream(), "Analyzers");
                if (input.TryGet("Analyzers", out BlittableJsonReaderArray analyzers) == false)
                    ThrowRequiredPropertyNameInRequest("Analyzers");

                var commands = new List<PutServerWideAnalyzerCommand>();
                foreach (var analyzerToAdd in analyzers)
                {
                    var analyzerDefinition = JsonDeserializationServer.AnalyzerDefinition((BlittableJsonReaderObject)analyzerToAdd);
                    analyzerDefinition.Name = analyzerDefinition.Name?.Trim();

                    if (RavenLogManager.Instance.IsAuditEnabled)
                    {
                        LogAuditFor("Server", "PUT", $"Analyzer '{analyzerDefinition.Name}' with definition: {analyzerToAdd}");
                    }

                    analyzerDefinition.Validate();

                    // check if analyzer is compilable
                    AnalyzerCompiler.Compile(analyzerDefinition.Name, analyzerDefinition.Code);

                    var command = new PutServerWideAnalyzerCommand(analyzerDefinition, GetRaftRequestIdFromQuery());

                    commands.Add(command);
                }

                var index = 0L;
                foreach (var command in commands)
                    index = (await ServerStore.SendToLeaderAsync(command)).Index;

                await ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, index);

                NoContentStatus(HttpStatusCode.Created);
            }
        }

        [RavenAction("/admin/analyzers", "DELETE", AuthorizationStatus.Operator)]
        public async Task Delete()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            if (RavenLogManager.Instance.IsAuditEnabled)
            {
                LogAuditFor("Server", "DELETE", $"Analyzer '{name}'");
            }

            var command = new DeleteServerWideAnalyzerCommand(name, GetRaftRequestIdFromQuery());
            var index = (await ServerStore.SendToLeaderAsync(command)).Index;

            await ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, index);

            NoContentStatus();
        }
    }
}
