using System;
using System.Net.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Identities
{
    public class NextIdentityForOperation : IMaintenanceOperation<long>
    {
        private readonly string _identityName;

        public NextIdentityForOperation(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException($"The field {nameof(name)} cannot be null or whitespace.");

            _identityName = name;
        }

        public RavenCommand<long> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new NextIdentityForCommand(_identityName);
        }
    }
}
