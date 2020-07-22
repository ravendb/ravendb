using System;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;
using Raven.Client.Util;
using Sparrow.Json;

namespace Tests.Infrastructure
{
    public class CreateSampleDataOperation : IMaintenanceOperation
    {
        private readonly DatabaseItemType _operateOnTypes;

        public CreateSampleDataOperation(DatabaseItemType operateOnTypes = DatabaseItemType.Documents)
        {
            _operateOnTypes = operateOnTypes;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new CreateSampleDataCommand(_operateOnTypes);
        }

        private class CreateSampleDataCommand : RavenCommand, IRaftCommand
        {
            private readonly DatabaseItemType _operateOnTypes;

            public CreateSampleDataCommand(DatabaseItemType operateOnTypes)
            {
                _operateOnTypes = operateOnTypes;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var sb = new StringBuilder($"{node.Url}/databases/{node.Database}/studio/sample-data");

                var operateOnTypes = _operateOnTypes.ToString().Split(",", StringSplitOptions.RemoveEmptyEntries);
                for (var i = 0; i < operateOnTypes.Length; i++)
                {
                    sb.Append(i == 0 ? "?" : "&");
                    sb.Append("operateOnTypes=");
                    sb.Append(operateOnTypes[i].Trim());
                }

                url = sb.ToString();

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post
                };
            }

            public string RaftUniqueRequestId { get; } = RaftIdGenerator.NewId();
        }
    }
}
