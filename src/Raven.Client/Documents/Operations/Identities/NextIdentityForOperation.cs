using System;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Identities
{
    /// <summary>
    /// Operation to generate the next identity value for a specified identity name in the database.
    /// </summary>
    public sealed class NextIdentityForOperation : IMaintenanceOperation<long>
    {
        private readonly string _identityName;

        /// <inheritdoc cref="NextIdentityForOperation"/>
        /// <param name="name">The name of the identity for which to generate the next value.</param>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="name"/> is null or whitespace.</exception>
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
