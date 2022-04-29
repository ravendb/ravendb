using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Documents.Sharding.Handlers.Processors.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedDocumentHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/docs", "HEAD")]
        public async Task Head()
        {
            using (var processor = new ShardedDocumentHandlerProcessorForHead(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenShardedAction("/databases/*/docs/size", "GET")]
        public async Task GetDocSize()
        {
            using (var processor = new ShardedDocumentHandlerProcessorForGetDocSize(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenShardedAction("/databases/*/docs", "GET")]
        public async Task Get()
        {
            using (var processor = new ShardedDocumentHandlerProcessorForGet(HttpMethod.Get, this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenShardedAction("/databases/*/docs", "POST")]
        public async Task PostGet()
        {
            using (var processor = new ShardedDocumentHandlerProcessorForGet(HttpMethod.Post, this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenShardedAction("/databases/*/docs", "DELETE")]
        public async Task Delete()
        {
            using (var processor = new ShardedDocumentHandlerProcessorForDelete(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenShardedAction("/databases/*/docs", "PUT")]
        public async Task Put()
        {
            using (var processor = new ShardedDocumentHandlerProcessorForPut(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenShardedAction("/databases/*/docs", "PATCH")]
        public async Task Patch()
        {
            var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
            var isTest = GetBoolValueQueryString("test", required: false) ?? false;
            var debugMode = GetBoolValueQueryString("debug", required: false) ?? isTest;
            var skipPatchIfChangeVectorMismatch = GetBoolValueQueryString("skipPatchIfChangeVectorMismatch", required: false) ?? false;

            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var patch = await context.ReadForMemoryAsync(RequestBodyStream(), "ScriptedPatchRequest");

                var index = DatabaseContext.GetShardNumber(context, id);

                var cmd = new ShardedCommand(this, Headers.IfMatch, content: patch);

                await DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(context, cmd, index);
                HttpContext.Response.StatusCode = (int)cmd.StatusCode;
                await cmd.Result.WriteJsonToAsync(ResponseBodyStream());
            }
        }

        [RavenShardedAction("/databases/*/docs/class", "GET")]
        public async Task GenerateClassFromDocument()
        {
            var id = GetStringQueryString("id");
            var lang = (GetStringQueryString("lang", required: false) ?? "csharp")
                .Trim().ToLowerInvariant();

            using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var index = DatabaseContext.GetShardNumber(context, id);

                var cmd = new ShardedCommand(this, Headers.None);
                await DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(context, cmd, index);
                var document = cmd.Result;
                if (document == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                switch (lang)
                {
                    case "csharp":
                        break;
                    default:
                        throw new NotImplementedException($"Document code generator isn't implemented for {lang}");
                }

                await using (var writer = new StreamWriter(ResponseBodyStream()))
                {
                    var codeGenerator = new JsonClassGenerator(lang);
                    var code = codeGenerator.Execute(document);
                    await writer.WriteAsync(code);
                }
            }
        }
    }
}
