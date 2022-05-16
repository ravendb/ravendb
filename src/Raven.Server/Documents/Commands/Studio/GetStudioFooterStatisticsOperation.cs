using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.Documents.Studio;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands.Studio
{
    public class GetStudioFooterStatisticsOperation : IMaintenanceOperation<FooterStatistics>
    {
        public RavenCommand<FooterStatistics> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetStudioFooterStatisticsCommand();
        }

        internal class GetStudioFooterStatisticsCommand : RavenCommand<FooterStatistics>
        {
            public override bool IsReadRequest => true;


            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/studio/footer/stats";

                var request = new HttpRequestMessage { Method = HttpMethod.Get };

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = JsonDeserializationServer.FooterStatistics(response);
            }
        }

    }
}
