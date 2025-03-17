using System;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Identities
{
    /// <summary>
    /// Operation to seed an identity value for a specified identity name in the database.
    /// </summary>
    public sealed class SeedIdentityForOperation : IMaintenanceOperation<long>
    {
        private readonly string _identityName;
        private readonly long _identityValue;
        private readonly bool _forceUpdate;

        /// <inheritdoc cref="SeedIdentityForOperation"/>
        /// <param name="name">The name of the identity to seed.</param>
        /// <param name="value">The value to set for the identity.</param>
        /// <param name="forceUpdate">
        /// If <c>true</c>, forces an update even if the current identity value is greater than the provided value.
        /// </param>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="name"/> is null or whitespace.</exception>
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
