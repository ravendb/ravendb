using System;
using System.Diagnostics;
using System.IO;
using JetBrains.Annotations;
using Raven.Server.Commercial;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Web;
using Sparrow;

namespace Raven.Server.Documents.AI.AiAssistant.Handlers.Processors;

internal abstract class AiAssistantHandlerProcessorBase : AbstractHandlerProcessor<RequestHandler>
{
    private readonly License _license;
    private readonly string _certificateThumbprint;

    protected AiAssistantHandlerProcessorBase([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
        // TODO Grisha
        var licenseExists = ServerStore.LicenseManager.TryGetLicense(out _license);

        if (licenseExists == false)
            throw new InvalidOperationException($"AI Assistant is available only for licenses instances of RavenDB. Please register your license.");

        _certificateThumbprint = RequestHandler.GetCurrentCertificate()?.Thumbprint ?? "unsecured node";
    }

    protected void FulfillRequestMetadata<TRequest>(TRequest request) where TRequest : AiAssistantRequestAuthentication
    {
        PortableExceptions.ThrowIf<InvalidDataException>(request.License is not null, $"License should be set by the server.");
        PortableExceptions.ThrowIf<InvalidDataException>(request.CertificateThumbprint is not null, $"Certificate thumbprint should be set by the server.");
        
        request.License = _license;
        request.CertificateThumbprint = _certificateThumbprint;
    }
}
