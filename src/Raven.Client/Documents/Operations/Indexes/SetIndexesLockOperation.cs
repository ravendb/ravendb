using System;
using System.Linq;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Indexes
{
    /// <summary>
    /// <para>Modifies the lock mode of one or more indexes using the SetIndexesLockOperation.</para>
    /// The lock mode controls how index modifications are handled and can only be applied to static indexes, not auto-indexes.
    /// <para><strong>Note:</strong> The lock mode is updated on all nodes within the database group.</para>
    /// </summary>
    public sealed class SetIndexesLockOperation : IMaintenanceOperation
    {
        private readonly Parameters _parameters;

        /// <inheritdoc cref="SetIndexesLockOperation" />
        /// <param name="indexName">The name of the index for which the lock mode is being modified.</param>
        /// <param name="mode">The lock mode to be set for the index. Valid values are Unlock, LockedIgnore, and LockedError.</param>
        public SetIndexesLockOperation(string indexName, IndexLockMode mode)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));

            _parameters = new Parameters
            {
                IndexNames = new[] { indexName },
                Mode = mode
            };

            FilterAutoIndexes();
        }

        /// <inheritdoc cref="SetIndexesLockOperation" />
        /// <param name="parameters">The Parameters object containing the list of index names and the lock mode to apply.</param>
        public SetIndexesLockOperation(Parameters parameters)
        {
            if (parameters == null)
                throw new ArgumentNullException(nameof(parameters));

            if (parameters.IndexNames == null || parameters.IndexNames.Length == 0)
                throw new ArgumentNullException(nameof(parameters.IndexNames));

            _parameters = parameters;

            FilterAutoIndexes();
        }

        private void FilterAutoIndexes()
        {
            // Check for auto-indexes - we do not set lock for auto-indexes
            if (_parameters.IndexNames.Any(indexName => indexName.StartsWith("Auto/", StringComparison.OrdinalIgnoreCase)))
            {
                throw new InvalidOperationException("'Indexes list contains Auto-Indexes. Lock Mode' is not set for Auto-Indexes.");
            }
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new SetIndexLockCommand(conventions, context, _parameters);
        }

        internal sealed class SetIndexLockCommand : RavenCommand, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly BlittableJsonReaderObject _parameters;

            public SetIndexLockCommand(DocumentConventions conventions, JsonOperationContext context, Parameters parameters)
            {
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                if (parameters == null)
                    throw new ArgumentNullException(nameof(parameters));
                _conventions = conventions ?? throw new ArgumentNullException(nameof(conventions));

                _parameters = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(parameters, context);
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/indexes/set-lock";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _parameters).ConfigureAwait(false), _conventions)
                };
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }

        /// <summary>
        /// Represents the parameters required to set the lock mode for multiple indexes.
        /// This class includes the list of index names and the lock mode to apply.
        /// </summary>
        public sealed class Parameters
        {
            /// <summary>
            /// An array of index names for which the lock mode is being modified.
            /// </summary>
            public string[] IndexNames { get; set; }
            /// <summary>
            /// The lock mode to be applied to the specified indexes. Valid values are Unlock, LockedIgnore, and LockedError.
            /// </summary>
            public IndexLockMode Mode { get; set; }
        }
    }
}
