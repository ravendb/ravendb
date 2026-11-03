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

        // nothing to send: the response stays as it was, so an error that follows still sets its status
        if (length == 0)
            return true;

        // PERF: compressing a small response costs more CPU than the bytes it saves are worth
        if (length < minResponseSizeToCompress)
            httpContext.Features.Set(SkipResponseCompressionFeature.Instance);
        else
            httpContext.Features.Set(new ResponseContentLengthFeature(length));

        // the response is completed (and so flushed) when the handler returns
        return true;
    }

    /// <summary>
    /// A writer passed content without flushing it: the response has not started, but an error that follows must be appended
    /// to the content, not replace its status.
    /// </summary>
    internal static bool HasContent(HttpContext httpContext) =>
        httpContext.Features.Get<SkipResponseCompressionFeature>() != null || httpContext.Features.Get<ResponseContentLengthFeature>() != null;
}
