using System;
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
    /// <para>Adjusts the priority of index threads using the SetIndexesPriorityOperation.
    /// Each index operates on its own dedicated thread, and this operation allows you to raise or lower the thread's priority.</para>
    /// By default, RavenDB assigns a lower priority to indexing threads compared to request-processing threads.
    /// 
    /// <para><strong>Indexes scope:</strong> The priority can be set for both static and auto indexes.</para>
    /// <para><strong>Nodes scope:</strong> The priority is updated on all nodes within the database group.</para>
    /// </summary>
    public sealed class SetIndexesPriorityOperation : IMaintenanceOperation
    {
        private readonly Parameters _parameters;

        /// <inheritdoc cref="SetIndexesPriorityOperation" />
        /// <param name="indexName">The name of the index for which the priority is being modified.</param>
        /// <param name="priority">The priority level to set for the index. Valid values are Low, Normal, and High.</param>
        public SetIndexesPriorityOperation(string indexName, IndexPriority priority)
        {
            if (indexName == null)
                throw new ArgumentNullException(nameof(indexName));

            _parameters = new Parameters
            {
                IndexNames = new[] { indexName },
                Priority = priority
            };
        }

        /// <inheritdoc cref="SetIndexesPriorityOperation" />
        /// <param name="parameters">The Parameters object containing the list of index names and the priority level to apply.</param>
        public SetIndexesPriorityOperation(Parameters parameters)
        {
            if (parameters == null)
                throw new ArgumentNullException(nameof(parameters));

            if (parameters.IndexNames == null || parameters.IndexNames.Length == 0)
                throw new ArgumentNullException(nameof(parameters.IndexNames));

            _parameters = parameters;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new SetIndexPriorityCommand(conventions, context, _parameters);
        }

        private sealed class SetIndexPriorityCommand : RavenCommand, IRaftCommand
        {
            private readonly DocumentConventions _conventions;
            private readonly BlittableJsonReaderObject _parameters;

            public SetIndexPriorityCommand(DocumentConventions conventions, JsonOperationContext context, Parameters parameters)
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
                url = $"{node.Url}/databases/{node.Database}/indexes/set-priority";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _parameters).ConfigureAwait(false), _conventions)
                };
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }

        /// <summary>
        /// Represents the parameters required to set the priority level for multiple indexes.
        /// This class includes the list of index names and the priority level to apply.
        /// </summary>
        public sealed class Parameters
        {
            /// <summary>
            /// An array of index names for which the priority is being modified.
            /// </summary>
            public string[] IndexNames { get; set; }
            /// <summary>
            /// The priority level to apply to the specified indexes. Valid values are Low, Normal, and High.
            /// </summary>
            public IndexPriority Priority { get; set; }
        }
    }
}
