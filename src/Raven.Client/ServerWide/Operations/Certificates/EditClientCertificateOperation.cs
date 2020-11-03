using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.Certificates
{
    public class EditClientCertificateOperation : IServerOperation
    {
        public class Parameters
        {
            public string Thumbprint { get; set; }
            public Dictionary<string, DatabaseAccess> Permissions { get; set; }
            public string Name { get; set; }
            public SecurityClearance Clearance { get; set; }
        }

        private readonly string _thumbprint;
        private readonly Dictionary<string, DatabaseAccess> _permissions;
        private readonly string _name;
        private readonly SecurityClearance _clearance;

        public EditClientCertificateOperation(Parameters parameters)
        {
            if (parameters == null)
                throw new ArgumentNullException(nameof(parameters));
            _name = parameters.Name ?? throw new ArgumentNullException(nameof(parameters.Name));
            _thumbprint = parameters.Thumbprint ?? throw new ArgumentNullException(nameof(parameters.Thumbprint));
            _permissions = parameters.Permissions ?? throw new ArgumentNullException(nameof(parameters.Permissions));
            _clearance = parameters.Clearance;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new EditClientCertificateCommand(_thumbprint, _name, _permissions, _clearance);
        }

        private class EditClientCertificateCommand : RavenCommand, IRaftCommand
        {
            private readonly string _thumbprint;
            private readonly Dictionary<string, DatabaseAccess> _permissions;
            private readonly string _name;
            private readonly SecurityClearance _clearance;

            public EditClientCertificateCommand(string thumbprint, string name, Dictionary<string, DatabaseAccess> permissions, SecurityClearance clearance)
            {
                _thumbprint = thumbprint;
                _name = name;
                _permissions = permissions;
                _clearance = clearance;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/certificates/edit";

                var definition = new CertificateDefinition
                {
                    Thumbprint = _thumbprint,
                    Permissions = _permissions,
                    SecurityClearance = _clearance,
                    Name = _name
                };
                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(stream => ctx.Write(stream, EntityToBlittable.ConvertCommandToBlittable(definition, ctx)))
                };

                return request;
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
