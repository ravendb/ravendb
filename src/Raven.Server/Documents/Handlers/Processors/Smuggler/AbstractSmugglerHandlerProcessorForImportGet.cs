using System;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Exceptions.Security;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Documents.Handlers;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Smuggler;

internal abstract class AbstractSmugglerHandlerProcessorForImportGet<TRequestHandler, TOperationContext> : AbstractSmugglerHandlerProcessorForImport<TRequestHandler, TOperationContext>
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TOperationContext : JsonOperationContext
{
    protected AbstractSmugglerHandlerProcessorForImportGet([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract ImportDelegate DoImport { get; }

    protected abstract long GetOperationId();

    protected override async ValueTask ImportAsync(JsonOperationContext context, long? operationId)
    {
        if (HttpContext.Request.Query.ContainsKey("file") == false &&
            HttpContext.Request.Query.ContainsKey("url") == false)
        {
            throw new ArgumentException("'file' or 'url' are mandatory when using GET /smuggler/import");
        }

        operationId ??= GetOperationId();
        var options = DatabaseSmugglerOptionsServerSide.Create(HttpContext);
        await using (var file = await GetImportStream())
        await using (var stream = new GZipStream(new BufferedStream(file, 128 * Voron.Global.Constants.Size.Kilobyte), CompressionMode.Decompress))
        using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
        {
            var result = await DoImport(context, stream, options, result: null, onProgress: null, operationId.Value, token);
            await WriteSmugglerResultAsync(context, result, RequestHandler.ResponseBodyStream());
        }
    }

    private async Task<Stream> GetImportStream()
    {
        var file = RequestHandler.GetStringQueryString("file", required: false);
        if (string.IsNullOrEmpty(file) == false)
        {
            if (await RequestHandler.IsOperatorAsync() == false)
                throw new AuthorizationException("The use of the 'file' query string parameters is limited operators and above");
            return File.OpenRead(file);
        }

        var url = RequestHandler.GetStringQueryString("url", required: false);
        if (string.IsNullOrEmpty(url) == false)
        {
            if (await RequestHandler.IsOperatorAsync() == false)
                    throw new AuthorizationException("The use of the 'url' query string parameters is limited operators and above");

            if (HttpContext.Request.Method == "POST")
            {
                var msg = await SmugglerHandler.HttpClient.PostAsync(url,
                    new StreamContent(HttpContext.Request.Body)
                    {
                        Headers = { ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue(HttpContext.Request.ContentType) }
                    });
                return await msg.Content.ReadAsStreamAsync();
            }

            return await SmugglerHandler.HttpClient.GetStreamAsync(url);
        }

        return HttpContext.Request.Body;
    }
}
