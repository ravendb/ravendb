using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.ETL
{
    public class ResetEtlOperation : IMaintenanceOperation
    {
        private readonly string _configurationName;
        private readonly string _transformationName;

        public ResetEtlOperation(string configurationName, string transformationName)
        {
            _configurationName = configurationName;
            _transformationName = transformationName;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ResetEtlCommand(_configurationName, _transformationName);
        }

        public class ResetEtlCommand : RavenCommand
        {
            private readonly string _configurationName;
            private readonly string _transformationName;

            public ResetEtlCommand(string configurationName, string transformationName)
            {
                _configurationName = configurationName;
                _transformationName = transformationName;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/etl?configurationName={_configurationName}&transformationName={_transformationName}";

                var request = new HttpRequestMessage
                {
                    Method = HttpMethods.Reset,
                    Content = new StringContent("{}")
                };

                return request;
            }
        }
    }
}
