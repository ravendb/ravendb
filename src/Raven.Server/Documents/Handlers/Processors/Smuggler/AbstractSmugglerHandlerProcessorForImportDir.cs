using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Handlers.Processors.Smuggler;

internal abstract class AbstractSmugglerHandlerProcessorForImportDir<TRequestHandler, TOperationContext> : AbstractSmugglerHandlerProcessorForImport<TRequestHandler, TOperationContext>
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TOperationContext : JsonOperationContext
{
    protected AbstractSmugglerHandlerProcessorForImportDir([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract ImportDelegate DoImport { get; }

    protected abstract long GetOperationId();

    protected override async ValueTask ImportAsync(JsonOperationContext context, long? operationId)
    {
        var extension = RequestHandler.GetStringQueryString("extension", required: false) ?? "dump";

        var directory = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("dir");
        var files = new BlockingCollection<Func<Task<Stream>>>(new ConcurrentQueue<Func<Task<Stream>>>(
            Directory.GetFiles(directory, $"*.{extension}")
                .Select(x => (Func<Task<Stream>>)(() => Task.FromResult<Stream>(File.OpenRead(x)))))
        );

        files.CompleteAdding();
        operationId ??= GetOperationId();
        await BulkImport(files, directory, operationId.Value);
    }

    private async Task BulkImport(BlockingCollection<Func<Task<Stream>>> files, string directory, long operationId)
    {
        var maxTasks = RequestHandler.GetIntValueQueryString("maxTasks", required: false) ?? ProcessorInfo.ProcessorCount / 2;
        var results = new ConcurrentQueue<SmugglerResult>();
        var tasks = new Task[Math.Max(1, maxTasks)];

        var finalResult = new SmugglerResult();
        var token = RequestHandler.CreateHttpRequestBoundOperationToken();
        var options = new DatabaseSmugglerOptionsServerSide(RequestHandler.GetAuthorizationStatusForSmuggler(RequestHandler.DatabaseName));
        for (int i = 0; i < tasks.Length; i++)
        {
            tasks[i] = Task.Run(async () =>
            {
                while (files.IsCompleted == false)
                {
                    Func<Task<Stream>> getFile;
                    try
                    {
                        getFile = files.Take();
                    }
                    catch (Exception)
                    {
                        continue;
                    }

                    using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
                    {
                        await using (var file = await getFile())
                        await using (var stream = await Utils.BackupUtils.GetDecompressionStreamAsync(new BufferedStream(file, 128 * Voron.Global.Constants.Size.Kilobyte)))
                        {
                            var result = await DoImport(context, stream, options: options, result: null, onProgress: null, operationId, token);
                            results.Enqueue(result);
                        }
                    }
                }
            });
        }

        await Task.WhenAll(tasks);

        while (results.TryDequeue(out SmugglerResult importResult))
        {
            ((IOperationResult)importResult).MergeWith(finalResult);
        }

        using (ContextPool.AllocateOperationContext(out JsonOperationContext finalContext))
        using (var memoryStream = RecyclableMemoryStreamFactory.GetRecyclableStream())
        {
            await WriteSmugglerResultAsync(finalContext, finalResult, memoryStream);
            memoryStream.Position = 0;
            try
            {
                await using (var output = File.Create(Path.Combine(directory, "smuggler.results.txt")))
                {
                    await memoryStream.CopyToAsync(output);
                }
            }
            catch (Exception)
            {
                // ignore any failure here
            }

            memoryStream.Position = 0;
            await memoryStream.CopyToAsync(RequestHandler.ResponseBodyStream());
        }
    }
}
