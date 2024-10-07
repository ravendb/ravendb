using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using Microsoft.Net.Http.Headers;

namespace Raven.Server.Documents.Handlers.Processors.Attachments
{
    internal static class RangeHelper
    {
        public static (bool SendBody, long Start, long? Length) SetRangeHeaders(HttpContext context, long size)
        {
            var responseHeaders = context.Response.GetTypedHeaders();
            context.Response.Headers.AcceptRanges = "bytes";

            long start = 0;
            var (isRangeRequest, range) = ParseRange(context, size);
            if (isRangeRequest)
            {
                if (range is null)
                {
                    context.Response.StatusCode = (int)HttpStatusCode.RequestedRangeNotSatisfiable;
                    responseHeaders.ContentRange = new ContentRangeHeaderValue(size);
                    responseHeaders.ContentLength = 0;
                    return (false, start, null);
                }

                context.Response.StatusCode = (int)HttpStatusCode.PartialContent;
                responseHeaders.ContentRange = ComputeContentRange(range, size, out start, out var length);
                responseHeaders.ContentLength = length;
                return (true, start, length);
            }
            else
            {
                responseHeaders.ContentLength = size;
                return (true, 0, null);
            }
        }

        private static (bool IsRangeRequest, RangeItemHeaderValue Range) ParseRange(HttpContext context, long length)
        {
            var rawRangeHeader = context.Request.Headers.Range;
            if (StringValues.IsNullOrEmpty(rawRangeHeader))
            {
                return (false, null);
            }

            // Perf: Check for a single entry before parsing it
            if (rawRangeHeader.Count > 1 || (rawRangeHeader[0] ?? string.Empty).Contains(','))
            {
                // The spec allows for multiple ranges but we choose not to support them because the client may request
                // very strange ranges (e.g. each byte separately, overlapping ranges, etc.) that could negatively
                // impact the server. Ignore the header and serve the response normally.
                return (false, null);
            }

            var rangeHeader = context.Request.GetTypedHeaders().Range;
            if (rangeHeader == null)
            {
                // Invalid
                return (false, null);
            }

            // Already verified above
            Debug.Assert(rangeHeader.Ranges.Count == 1);

            var ranges = rangeHeader.Ranges;
            if (ranges == null)
            {
                return (false, null);
            }

            if (ranges.Count == 0)
            {
                return (true, null);
            }

            if (length == 0)
            {
                return (true, null);
            }

            // Normalize the ranges
            var range = NormalizeRange(ranges.Single(), length);

            // Return the single range
            return (true, range);
        }


        // Note: This assumes ranges have been normalized to absolute byte offsets.
        private static ContentRangeHeaderValue ComputeContentRange(RangeItemHeaderValue range, long size, out long start, out long length)
        {
            start = range.From!.Value;
            var end = range.To!.Value;
            length = end - start + 1;
            return new ContentRangeHeaderValue(start, end, size);
        }

        private static RangeItemHeaderValue NormalizeRange(RangeItemHeaderValue range, long length)
        {
            var start = range.From;
            var end = range.To;

            // X-[Y]
            if (start.HasValue)
            {
                if (start.Value >= length)
                {
                    // Not satisfiable, skip/discard.
                    return null;
                }
                if (!end.HasValue || end.Value >= length)
                {
                    end = length - 1;
                }
            }
            else if (end.HasValue)
            {
                // suffix range "-X" e.g. the last X bytes, resolve
                if (end.Value == 0)
                {
                    // Not satisfiable, skip/discard.
                    return null;
                }

                var bytes = Math.Min(end.Value, length);
                start = length - bytes;
                end = start + bytes - 1;
            }

            return new RangeItemHeaderValue(start, end);
        }
    }
}
