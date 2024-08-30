using System;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    /// <summary>
    /// In dynamic database distribution mode, if a database node is down, another cluster node is added to the database group to compensate.
    /// Use this operation to toggle dynamic distribution for a particular database group.
    /// </summary>
    /// <inheritdoc cref="DocumentationUrls.Operations.ServerOperations.SetDatabaseDynamicDistributionOperation"/>
    public sealed class SetDatabaseDynamicDistributionOperation : IServerOperation
    {
        private readonly bool _allowDynamicDistribution;
        private readonly string _databaseName;
        
        /// <inheritdoc cref="SetDatabaseDynamicDistributionOperation"/>
        /// <param name="databaseName">Name of database group</param>
        /// <param name="allowDynamicDistribution">Set to true to activate dynamic distribution mode.</param>
        /// <exception cref="ArgumentException">Thrown when <paramref name="databaseName"/> is null or empty.</exception>
        public SetDatabaseDynamicDistributionOperation(string databaseName, bool allowDynamicDistribution)
        {
            if (string.IsNullOrEmpty(databaseName))
            {
                throw new ArgumentException("databaseName should not be null or empty");
            }
            _allowDynamicDistribution = allowDynamicDistribution;
            _databaseName = databaseName;
        }


        RavenCommand IServerOperation.GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new SetDatabaseDynamicDistributionCommand(_databaseName, _allowDynamicDistribution);
        }

        private sealed class SetDatabaseDynamicDistributionCommand : RavenCommand, IRaftCommand
        {
            private readonly string _databaseName;
            private readonly bool _allowDynamicDistribution;

            public SetDatabaseDynamicDistributionCommand(string databaseName, bool allowDynamicDistribution)
            {
                _databaseName = databaseName;
                _allowDynamicDistribution = allowDynamicDistribution;
            }

            public override bool IsReadRequest => false;
            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {

                url = $"{node.Url}/admin/databases/dynamic-node-distribution?name={_databaseName}&enable={_allowDynamicDistribution}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }

            public string RaftUniqueRequestId => RaftIdGenerator.NewId();
        }
    }


}
