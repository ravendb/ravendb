using System;
using System.Net.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Identities
{
    public class SeedIdentityForOperation : IMaintenanceOperation<long>
    {
        private readonly string _identityName;
        private readonly long _identityValue;
        private readonly bool _forceUpdate;

        public SeedIdentityForOperation(string name, long value, bool forceUpdate = false)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException($"The field {nameof(name)} cannot be null or whitespace.");

            _identityName = name;
            _identityValue = value;
            _forceUpdate = forceUpdate;
        }

        public RavenCommand<long> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new SeedIdentityForCommand(_identityName, _identityValue, _forceUpdate);
        }
    }
}
