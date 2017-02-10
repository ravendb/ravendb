using System.Net.Http;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Http;
using Sparrow.Json;

namespace Raven.NewClient.Operations.Databases
{
    public class GetDatabaseNamesOperation : IAdminOperation<string[]>
    {
        private readonly int _start;
        private readonly int _pageSize;

        public GetDatabaseNamesOperation(int start, int pageSize)
        {
            _start = start;
            _pageSize = pageSize;
        }

        public RavenCommand<string[]> GetCommand(DocumentConvention conventions, JsonOperationContext context)
        {
            return new GetDatabaseNamesCommand(_start, _pageSize);
        }

        private class GetDatabaseNamesCommand : RavenCommand<string[]>
        {
            private readonly int _start;
            private readonly int _pageSize;

            public GetDatabaseNamesCommand(int start, int pageSize)
            {
                _start = start;
                _pageSize = pageSize;
            }

            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/resources?start={_start}&pageSize={_pageSize}&namesOnly=true";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                BlittableJsonReaderArray names;
                if (response.TryGet(nameof(ResourcesInfo.Databases), out names) == false)
                    ThrowInvalidResponse();

                var result = new string[names.Length];
                for (var i = 0; i < names.Length; i++)
                    result[i] = names[i].ToString();

                Result = result;
            }
        }
    }
}