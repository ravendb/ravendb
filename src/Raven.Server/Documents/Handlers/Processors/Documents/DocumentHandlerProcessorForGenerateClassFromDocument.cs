using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Changes;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal sealed class DocumentHandlerProcessorForGenerateClassFromDocument : AbstractDocumentHandlerProcessorForGenerateClassFromDocument<DocumentHandler, DocumentsOperationContext>
{
    public DocumentHandlerProcessorForGenerateClassFromDocument([NotNull] DocumentHandler requestHandler) : base(requestHandler)
    {
    }

    protected override async ValueTask HandleClassGenerationAsync(string id, string lang)
    {
        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var document = RequestHandler.Database.DocumentsStorage.Get(context, id);
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

            await using (var writer = new StreamWriter(RequestHandler.ResponseBodyStream()))
            {
                var codeGenerator = new JsonClassGenerator(lang);
                var code = codeGenerator.Execute(document.Data);
                await writer.WriteAsync(code);

                if (TrafficWatchManager.HasRegisteredClients)
                    RequestHandler.AddStringToHttpContext(writer.ToString(), TrafficWatchChangeType.Documents);
            }
        }
    }
}
