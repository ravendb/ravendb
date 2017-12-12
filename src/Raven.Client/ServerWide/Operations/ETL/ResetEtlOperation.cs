using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Raven.Client.ServerWide.Operations.ETL
{
    public class ResetEtlOperation : IServerOperation
    {
        private readonly string _configurationName;
        private readonly string _transformationName;
        private readonly string _databaseName;

        public ResetEtlOperation(string configurationName, string transformationName, string databaseName)
        {
            _configurationName = configurationName;
            _transformationName = transformationName;
            _databaseName = databaseName;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new ResetEtlCommand(_configurationName, _transformationName, _databaseName);
        }

        public class ResetEtlCommand : RavenCommand
        {
            private readonly string _databaseName;
            private readonly string _configurationName;
            private readonly string _transformationName;

            public ResetEtlCommand(string configurationName, string transformationName, string databaseName)
            {
                _configurationName = configurationName;
                _transformationName = transformationName;
                _databaseName = databaseName;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{_databaseName}/admin/etl?configurationName={_configurationName}&transformationName={_transformationName}";

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
