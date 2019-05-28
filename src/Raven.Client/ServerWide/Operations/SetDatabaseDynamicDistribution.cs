using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations
{
    public class SetDatabaseDynamicDistribution : IServerOperation<bool>
    {
        private bool _allowDynamicDistribution;
        private string _databaseName;

        public SetDatabaseDynamicDistribution(string databaseName, bool allowDynamicDistribution)
        {
            _allowDynamicDistribution = allowDynamicDistribution;
            _databaseName = databaseName;
        }

        public RavenCommand<bool> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new SetDatabaseDynamicDistributionCommand(_databaseName, _allowDynamicDistribution);
        }
    }

    public class SetDatabaseDynamicDistributionCommand : RavenCommand<bool>, IRaftCommand
    {
        private string _databaseName;
        private bool _allowDynamicDistribution;

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

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            Result = _allowDynamicDistribution;
        }

        public string RaftUniqueRequestId => RaftIdGenerator.NewId();
    }
}
