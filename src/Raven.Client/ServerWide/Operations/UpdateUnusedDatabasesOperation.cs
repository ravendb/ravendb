using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    /// <summary>
    ///  Allows to specify the IDs of database instances that no longer exist so that their entries can be removed from the database change vector.
    /// This operation does not affect any documents' current change vectors, but from now on when documents are modified or created their change vector will not include the obsolete entries.
    /// </summary>
    /// <inheritdoc cref="DocumentationUrls.Operations.ServerOperations.UpdateUnusedDatabasesOperation"/>
    public sealed class UpdateUnusedDatabasesOperation : IServerOperation
    {
        private readonly string _database;
        private readonly Parameters _parameters;

        /// <inheritdoc cref="UpdateUnusedDatabasesOperation"/>
        /// <param name="database">Name of the database</param>
        /// <param name="unusedDatabaseIds">The database IDs to be removed from the change vector</param>
        /// <exception cref="ArgumentException">Thrown when database is null or empty.</exception>
        public UpdateUnusedDatabasesOperation(string database, HashSet<string> unusedDatabaseIds)
        {
            if (string.IsNullOrEmpty(database))
                throw new ArgumentException(database);

            _database = database;
            _parameters = new Parameters
            {
                DatabaseIds = unusedDatabaseIds
            };
        }

        public UpdateUnusedDatabasesOperation(string database, HashSet<string> unusedDatabaseIds, bool validate) : this(database, unusedDatabaseIds)
        {
            _parameters.Validate = validate;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new UpdateUnusedDatabasesCommand(conventions, _database, _parameters);
        }

        private sealed class UpdateUnusedDatabasesCommand : RavenCommand, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly string _database;
            private readonly Parameters _parameters;

            public UpdateUnusedDatabasesCommand(DocumentConventions conventions, string database, Parameters parameters)
            {
                _conventions = conventions;
                _database = database;
                _parameters = parameters;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/databases/unused-ids?name={_database}";
                if (_parameters.Validate)
                    url += $"&validate=true";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_parameters, ctx)).ConfigureAwait(false), _conventions)
                };
            }

            public string RaftUniqueRequestId => RaftIdGenerator.NewId();
        }

        internal sealed class Parameters
        {
            public HashSet<string> DatabaseIds { get; set; }
            public bool Validate { get; set; }
        }
    }
}
