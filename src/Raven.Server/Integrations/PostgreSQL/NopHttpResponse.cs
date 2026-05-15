using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;

namespace Raven.Server.Integrations.PostgreSQL
{
    /// <summary>
    /// A no-op HttpResponse used when invoking the streaming query pipeline outside of an HTTP context
    /// (e.g. from the PostgreSQL wire-protocol integration). HasStarted always returns false so the
    /// StreamQueryResult guard never throws.
    /// </summary>
    internal sealed class NopHttpResponse : HttpResponse
    {
        public static readonly NopHttpResponse Instance = new();

        private NopHttpResponse() { }

        public override bool HasStarted => false;

        public override HttpContext HttpContext => throw new NotSupportedException();
        public override int StatusCode { get; set; }
        public override IHeaderDictionary Headers => new HeaderDictionary();
        public override Stream Body { get; set; } = Stream.Null;
        public override long? ContentLength { get; set; }
        public override string ContentType { get; set; }
        public override IResponseCookies Cookies => throw new NotSupportedException();

        public override void OnStarting(Func<object, Task> callback, object state) { }
        public override void OnCompleted(Func<object, Task> callback, object state) { }
        public override void Redirect(string location, bool permanent) { }
    }
}
