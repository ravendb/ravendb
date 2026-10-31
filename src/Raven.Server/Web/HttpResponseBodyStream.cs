using Microsoft.AspNetCore.Http;
using Raven.Client.Util;
using Raven.Server.Web.ResponseCompression;
using Sparrow.Json;

namespace Raven.Server.Web;

internal sealed class HttpResponseBodyStream(HttpContext httpContext, long minResponseSizeToCompress)
    : StreamWithTimeout(httpContext.Response.Body, RequestHandler.AbortConnection, httpContext), IContentLengthHint
{
    public bool OnContentLength(long length)
    {
        // part of the response is already out, too late to change how it is encoded
        if (TotalWritten != 0)
            return false;

        // PERF: compressing a small response costs more CPU than the bytes it saves are worth
        if (length < minResponseSizeToCompress)
            httpContext.Features.Set(SkipResponseCompressionFeature.Instance);
        else
            httpContext.Features.Set(new ResponseContentLengthFeature(length));

        // the response is completed (and so flushed) when the handler returns
        return true;
    }
}
