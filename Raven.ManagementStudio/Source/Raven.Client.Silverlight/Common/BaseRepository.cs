namespace Raven.Client.Silverlight.Common
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Net;
    using System.Threading;
    using Newtonsoft.Json.Linq;
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
            request = (HttpWebRequest)WebRequest.Create(new Uri(requestUri.AbsoluteUri + Guid.NewGuid())); // TODO [ppekrol] workaround for caching problem in SL
            request.Method = method.GetName();

            switch (method)
            {
                case RequestMethod.POST:
                    request.ContentType = "application/json";
                    break;
                default:
                    break;
            }

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

        protected string GetResponseStream(HttpWebRequest request, IAsyncResult result, out HttpStatusCode statusCode, out Exception exception)
        {
            try
            {
                var response = request.EndGetResponse(result) as HttpWebResponse;
                var stream = response.GetResponseStream();
                var reader = new StreamReader(stream);

                statusCode = response.StatusCode;
                exception = null;

                return reader.ReadToEnd();
            }
            catch (WebException ex)
            {
                var response = ex.Response as HttpWebResponse;
                if (response == null)
                {
                    throw;
                }

                statusCode = response.StatusCode;
                exception = this.ExtractException(response);

                return new JArray().ToString();
            }
            catch (Exception ex)
            {
                statusCode = HttpStatusCode.NotImplemented;
                exception = ex;

                return new JArray().ToString();
            }
        }

        private Exception ExtractException(HttpWebResponse response)
        {
            var stream = response.GetResponseStream();
            var reader = new StreamReader(stream);

            var json = reader.ReadToEnd();

            return new Exception(string.IsNullOrEmpty(json) ? string.Empty : JObject.Parse(json)["Error"].ToString());
        }
    }
}
