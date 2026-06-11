using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Sparrow.Json;

namespace Tests.Infrastructure.Operations
{
    public class GetClusterDebugInfoPackageOperation : IServerOperation<ClusterDebugInfoPackageResult>
    {
        public RavenCommand<ClusterDebugInfoPackageResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetDebugInfoPackageCommand();
        }

        private class GetDebugInfoPackageCommand : RavenCommand<ClusterDebugInfoPackageResult>
        {
            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/admin/debug/cluster-info-package";
                
                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override async Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
            {
                var contentDisposition = response.Content.Headers.TryGetValues(Constants.Headers.ContentDisposition, out var values) ? values.First() : null;
                var fileName = GetFileNameFromContentDisposition(contentDisposition);

                var stream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);

                Result = new ClusterDebugInfoPackageResult {Stream = stream, FileName = fileName};

                return ResponseDisposeHandling.Manually;
            }

            private string GetFileNameFromContentDisposition(string contentDisposition)
            {
                if (string.IsNullOrEmpty(contentDisposition))
                    return null;

                const string fileNamePrefix = "filename=";

                var fileNameEntry = contentDisposition.Split(';')
                    .Select(x => x.Trim())
                    .FirstOrDefault(x => x.StartsWith(fileNamePrefix, StringComparison.OrdinalIgnoreCase));

                var fileName = fileNameEntry?.Substring(fileNamePrefix.Length);
                return SanitizeFileName(fileName);
            }

            private string SanitizeFileName(string fileName) => fileName?.Replace(":", "");
        }
    }

    public class ClusterDebugInfoPackageResult
    {
        public string FileName { get; set; }
        public Stream Stream { get; set; } 
    }
}
