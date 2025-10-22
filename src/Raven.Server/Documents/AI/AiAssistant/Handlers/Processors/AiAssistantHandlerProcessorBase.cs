using System;
using JetBrains.Annotations;
using Raven.Server.Commercial;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.AI.AiAssistant.Handlers.Processors;

internal abstract class AiAssistantHandlerProcessorBase : AbstractHandlerProcessor<RequestHandler>
{
    private readonly License _license;
    private readonly string _certificateThumbprint;
    
    private const string LicensePropertyName = "License";
    private const string CertificateThumbprintPropertyName = "CertificateThumbprint";

    protected AiAssistantHandlerProcessorBase([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
        _license = ServerStore.LoadLicense();

        if (_license is null)
            throw new InvalidOperationException("AI Assistant is available only for licensed instances of RavenDB. Please register your license.");

        _certificateThumbprint = RequestHandler.GetCurrentCertificate()?.Thumbprint;
    }

    protected BlittableJsonReaderObject FulfillRequestMetadata(BlittableJsonReaderObject requestBody, TransactionOperationContext context)
    {
        requestBody.Modifications = new DynamicJsonValue()
        {
            [LicensePropertyName] = _license.ToJson(),
            [CertificateThumbprintPropertyName] = _certificateThumbprint,
        };

        requestBody = context.ReadObject(requestBody, null);
        requestBody.Modifications = null;

        return requestBody;
    }
}
