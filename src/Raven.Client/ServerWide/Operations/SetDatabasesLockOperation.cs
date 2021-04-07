using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class SetDatabasesLockOperation : IServerOperation
    {
        private readonly Parameters _parameters;

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

        private class SetDatabasesLockCommand : RavenCommand, IRaftCommand
        {
            private readonly BlittableJsonReaderObject _parameters;

            public SetDatabasesLockCommand(DocumentConventions conventions, JsonOperationContext context, Parameters parameters)
            {
                if (conventions == null)
                    throw new ArgumentNullException(nameof(conventions));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                if (parameters == null)
                    throw new ArgumentNullException(nameof(parameters));

                _parameters = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(parameters, context);
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/databases/set-lock";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _parameters).ConfigureAwait(false))
                };
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }

        public class Parameters
        {
            public string[] DatabaseNames { get; set; }
            public DatabaseLockMode Mode { get; set; }
        }
    }
}
