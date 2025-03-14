using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.Documents.ETL.Providers.AI.Extensions;

public class DimensionalityHandler : DelegatingHandler
{
    private const string BatchEmbedContents = ":batchEmbedContents";
    private const string EmbedContent = ":embedContent";
    private const string OutputDimensionality = "output_dimensionality";
    private const string Requests = "requests";
    private const string MediaType = "application/json";
    private const string BlittableDocumentId = "requestBody/json";

    private static readonly DocumentConventions ConventionsToUse = new() { UseHttpCompression = false };

    private readonly int _dimensions;

    public DimensionalityHandler(int dimensions, HttpMessageHandler innerHandler = null)
        : base(innerHandler ?? new HttpClientHandler())
    {
        _dimensions = dimensions;
    }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        if (request.Content == null || request.RequestUri == null)
            return await base.SendAsync(request, cancellationToken);

        var jsonString = await request.Content.ReadAsStringAsync(cancellationToken);
        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(jsonString)))
        {
            var blittable = context.Sync.ReadForMemory(stream, BlittableDocumentId);

            if (request.RequestUri.AbsolutePath.Contains(BatchEmbedContents))
            {
                if (blittable.TryGet(name: Requests, out BlittableJsonReaderArray requests) == false)
                    return await base.SendAsync(request, cancellationToken);

                foreach (BlittableJsonReaderObject innerRequest in requests)
                    innerRequest.Modifications = new DynamicJsonValue { [OutputDimensionality] = _dimensions };

                blittable.Modifications = new DynamicJsonValue { [Requests] = requests };
            }
            else if (request.RequestUri.AbsolutePath.Contains(EmbedContent))
            {
                blittable.Modifications = new DynamicJsonValue { [OutputDimensionality] = _dimensions };
            }

            var newBlittable = context.ReadObject(blittable, BlittableDocumentId);

            var content = new BlittableJsonContent(async s => await context.WriteAsync(s, newBlittable, cancellationToken), ConventionsToUse);
            content.Headers.ContentType = new MediaTypeHeaderValue(MediaType);
            request.Content = content;

            try
            {
                return await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                var requestBodyTask = content.EnsureCompletedAsync();

                if (requestBodyTask.IsCompleted == false)
                    await requestBodyTask.ConfigureAwait(false);
            }
        }
    }
}

public static class HttpClientExtensions
{
    public static HttpClient CreateWithDimensionality(int dimensions) => new(new DimensionalityHandler(dimensions));
}
