using System;
using System.IO;
using System.Net;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database;

namespace Raven.Client
{
    public class HttpJsonRequest
    {
        private readonly WebRequest webRequest;

        public HttpJsonRequest(string url, string method)
        {
            webRequest = WebRequest.Create(url);
            webRequest.Method = method;
            webRequest.ContentType = "application/json";
        }

        public string ReadResponseString()
        {
            var response = webRequest.GetResponse();
            using (var responseString = response.GetResponseStream())
            {
                var reader = new StreamReader(responseString);
                var text = reader.ReadToEnd();
                reader.Close();
                return text;
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