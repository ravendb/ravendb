using System;
using System.Diagnostics;
using System.Net.Http;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.ServerWide.DebugInfo
{
    public class GetRawResponseCommand : RavenCommand<BlittableJsonReaderObject>
    {
        public override bool IsReadRequest => true;

        private readonly string _method;
        private readonly string _path;

        public GetRawResponseCommand(string method, string path)
        {
            _method = method;
            _path = path;
        }

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            url = $"{node.Url}{_path}";

            switch (_method.Trim().ToLowerInvariant())
            {
                case "get":
                    return new HttpRequestMessage
                    {
                        Method = HttpMethod.Get,
                    };
                case "head":
                    return new HttpRequestMessage
                    {
                        Method = HttpMethod.Head,
                    };
                default:
                    throw new InvalidOperationException($"Expected to find either GET or HEAD debug endpoint, but found {_method}");
            }
        }

        public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
        {            
            Result = response;
        }
    }
}