using System;
using JetBrains.Annotations;
using Raven.Server.Commercial;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Web;
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

    protected void FulfillRequestMetadata(DynamicJsonValue requestBody)
    {
        requestBody[LicensePropertyName] = _license.ToJson();
        requestBody[CertificateThumbprintPropertyName] = _certificateThumbprint;
    }
}
