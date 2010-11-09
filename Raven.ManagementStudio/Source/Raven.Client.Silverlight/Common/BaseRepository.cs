namespace Raven.Client.Silverlight.Common
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Net;
    using System.Threading;
    using Raven.Client.Silverlight.Common.Helpers;
    using Raven.Client.Silverlight.Common.Mappers;
    using Raven.Client.Silverlight.Data;

    public class BaseRepository<T> where T : Entity
    {
        protected IDictionary<HttpWebRequest, SynchronizationContext> ContextStorage { get; set; }

        protected Uri DatabaseAddress { get; set; }

        protected IMapper<T> Mapper { get; set; }

        protected void CreateContext(Uri requestUri, RequestMethod method, out HttpWebRequest request)
        {
            request = (HttpWebRequest)WebRequest.Create(new Uri(requestUri.AbsoluteUri + "?" + Guid.NewGuid())); // TODO [ppekrol] workaround for caching problem in SL
            request.Method = method.GetName();

            this.ContextStorage.Add(request, SynchronizationContext.Current);
        }

        protected SynchronizationContext GetContext(HttpWebRequest request)
        {
            Guard.Assert(() => this.ContextStorage.ContainsKey(request));

            return this.ContextStorage[request];
        }

        protected void DeleteContext(HttpWebRequest request)
        {
            Guard.Assert(() => request != null);

            this.ContextStorage.Remove(request);
        }

        protected string GetResponseStream(HttpWebRequest request, IAsyncResult result, out HttpStatusCode statusCode)
        {
            var response = request.EndGetResponse(result) as HttpWebResponse;
            var stream = response.GetResponseStream();
            var reader = new StreamReader(stream);

            statusCode = response.StatusCode;

            return reader.ReadToEnd();
        }
    }
}
