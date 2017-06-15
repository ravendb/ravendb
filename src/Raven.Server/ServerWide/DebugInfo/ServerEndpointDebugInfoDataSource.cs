using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Server.Routing;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.DebugInfo
{
    public class ServerEndpointDebugInfoDataSource : IDebugInfoDataSource
    {
        private readonly RouteInformation _routeInformation;
        private readonly RequestExecutor _requestExecutor;
        private readonly CancellationToken _token;

        public ServerEndpointDebugInfoDataSource(string url, RouteInformation routeInformation, CancellationToken token)
        {
            _requestExecutor = RequestExecutor.CreateForSingleNode(url, null, null);        
            _routeInformation = routeInformation;
            _token = token;
        }

        public string FullPath
        {
            get
            {
                var filename = _routeInformation.Path;
                if (filename.StartsWith("debug/", StringComparison.OrdinalIgnoreCase))
                    filename = filename.Replace("debug/", string.Empty);
                if (filename.StartsWith("/debug/", StringComparison.OrdinalIgnoreCase))
                    filename = filename.Replace("/debug/", string.Empty);
                filename = filename.Replace("/", ".");

                return $"Server-wide\\{filename}.json";
            }
        }

        public async Task<BlittableJsonReaderObject> GetData(JsonOperationContext context)
        {
            var cmd = new GetRawResponseCommand(_routeInformation.Method, _routeInformation.Path);           
            await _requestExecutor.ExecuteAsync(cmd, context, _token);
            return cmd.Result;
        }

        public void Dispose()
        {
            _requestExecutor.Dispose();
        }
    }
}
