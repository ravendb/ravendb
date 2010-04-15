using System;
using System.Collections.Specialized;
using System.IO;
using System.Net;
using System.Text;
using Newtonsoft.Json.Linq;

namespace Raven.Client.Client
{
    public class HttpJsonRequest
    {
        private readonly WebRequest webRequest;

        public NameValueCollection ResponseHeaders { get; set; }

        public HttpJsonRequest(string url, string method)
            : this(url, method, new JObject())
        {
        }

        public HttpJsonRequest(string url, string method, JObject metadata)
        {
            webRequest = WebRequest.Create(url);
            WriteMetadata(metadata);
            webRequest.Method = method;
            webRequest.ContentType = "application/json";
        }

        public string ReadResponseString()
        {
            WebResponse response;
            try
            {
                response = webRequest.GetResponse();
            }
            catch (WebException e)
            {
                var httpWebResponse = e.Response as HttpWebResponse;
                if (httpWebResponse == null || 
                    httpWebResponse.StatusCode == HttpStatusCode.NotFound ||
                    httpWebResponse.StatusCode == HttpStatusCode.Conflict)
                    throw;
                using (var sr = new StreamReader(e.Response.GetResponseStream()))
                {
                    throw new InvalidOperationException(sr.ReadToEnd(), e);
                }
            }
            ResponseHeaders = response.Headers;
            using (var responseString = response.GetResponseStream())
            {
                var reader = new StreamReader(responseString);
                var text = reader.ReadToEnd();
                reader.Close();
                return text;
            }
        }

        private void WriteMetadata(JObject metadata)
        {
            if (metadata == null)
                return;

            foreach (JProperty prop in metadata)
            {
                if (prop.Value == null)
                    continue;

                if (prop.Value.Type == JsonTokenType.Object ||
                    prop.Value.Type == JsonTokenType.Array)
                    continue;

                var headerName = prop.Name;
                if (headerName == "ETag")
                    headerName = "If-Match";
                var value = prop.Value.Value<object>().ToString();
                switch (headerName)
                {
                    case "Content-Type":
                        webRequest.ContentType = value;
                        break;
                    default:
                        webRequest.Headers[headerName] = value;
                        break;
                }
            }
        }

        public void Write(string data)
        {
            using (var dataStream = webRequest.GetRequestStream())
            {
                var byteArray = Encoding.UTF8.GetBytes(data);
                dataStream.Write(byteArray, 0, byteArray.Length);
                dataStream.Close();
            }
        }
    }
}