using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Certificates
{
    /// <summary>
    /// Allows to delete a client certificate.
    /// </summary>
    /// <inheritdoc cref="DocumentationUrls.Operations.ServerOperations.DeleteCertificateOperation"/>
    public sealed class DeleteCertificateOperation : IServerOperation
    {
        private readonly string _thumbprint;

        /// <inheritdoc cref="DeleteCertificateOperation"/>
        /// <param name="thumbprint">Certificate thumbprint.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="thumbprint"/> is null.</exception>
        public DeleteCertificateOperation(string thumbprint)
        {
            _thumbprint = thumbprint ?? throw new ArgumentNullException(nameof(thumbprint));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteCertificateCommand(_thumbprint);
        }

        private sealed class DeleteCertificateCommand : RavenCommand, IRaftCommand
        {
            private readonly string _thumbprint;

            public DeleteCertificateCommand(string thumbprint)
            {
                _thumbprint = thumbprint ?? throw new ArgumentNullException(nameof(thumbprint));
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/certificates?thumbprint=" + Uri.EscapeDataString(_thumbprint);

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Delete
                };
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
