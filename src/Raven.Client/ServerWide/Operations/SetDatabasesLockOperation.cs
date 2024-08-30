using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    /// <summary>
    /// Operation modifies the lock mode for a database
    /// </summary>
    public sealed class SetDatabasesLockOperation : IServerOperation
    {
        private readonly Parameters _parameters;

        /// <inheritdoc cref="SetDatabasesLockOperation"/>
        /// <param name="databaseName">The name of the database</param>
        /// <param name="mode">Specify the database lock mode. See more at <see cref="DatabaseLockMode"/></param>
        /// <exception cref="ArgumentNullException">Thrown when database is null</exception>
        public SetDatabasesLockOperation(string databaseName, DatabaseLockMode mode)
        {
            if (databaseName == null)
                throw new ArgumentNullException(nameof(databaseName));

            _parameters = new Parameters
            {
                DatabaseNames = new[] { databaseName },
                Mode = mode
            };
        }

        public SetDatabasesLockOperation(Parameters parameters)
        {
            if (parameters == null)
                throw new ArgumentNullException(nameof(parameters));

            if (parameters.DatabaseNames == null || parameters.DatabaseNames.Length == 0)
                throw new ArgumentNullException(nameof(parameters.DatabaseNames));

            _parameters = parameters;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new SetDatabasesLockCommand(conventions, context, _parameters);
        }

        private sealed class SetDatabasesLockCommand : RavenCommand, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly BlittableJsonReaderObject _parameters;

            public SetDatabasesLockCommand(DocumentConventions conventions, JsonOperationContext context, Parameters parameters)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                if (parameters == null)
                    throw new ArgumentNullException(nameof(parameters));
                _conventions = conventions;

                _parameters = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(parameters, context);
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/databases/set-lock";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _parameters).ConfigureAwait(false), _conventions)
                };
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }

        public sealed class Parameters
        {
            public string[] DatabaseNames { get; set; }
            public DatabaseLockMode Mode { get; set; }
        }
    }
}
