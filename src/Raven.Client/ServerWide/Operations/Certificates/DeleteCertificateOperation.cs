using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Certificates
{
    public class DeleteCertificateOperation : IServerOperation
    {
        private readonly string _name;

        public DeleteCertificateOperation(string name)
        {
            _name = name ?? throw new ArgumentNullException(nameof(name));
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new DeleteCertificateCommand(_name);
        }

        private class DeleteCertificateCommand : RavenCommand
        {
            private readonly string _name;

            public DeleteCertificateCommand(string name)
            {
                _name = name ?? throw new ArgumentNullException(nameof(name));
            }

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/certificates?name=" + Uri.EscapeDataString(_name);

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Delete
                };
            }
        }
    }
}