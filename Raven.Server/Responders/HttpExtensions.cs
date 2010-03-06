using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Raven.Server.Responders
{
    public static class HttpExtensions
    {
        private static readonly HashSet<string> HeadersToIgnore = new HashSet<string>
        {
            // Entity headers - those are NOT ignored
            /*
            "Allow",
            "Content-Disposition",
            "Content-Encoding",
            "Content-Language",
            "Content-Location",
            "Content-MD5",
            "Content-Range",
            "Content-Type",
            "Expires",
            "Last-Modified",
             */
            // Ignoring this header, since it may
            // very well change due to things like encoding,
            // adding metadata, etc
            "Content-Length",
            // Special things to ignore
            "Keep-Alive",
            "X-Requested-With",
            // Request headers
            "Accept-Charset",
            "Accept-Encoding",
            "Accept",
            "Accept-Language",
            "Authorization",
            "Cookie",
            "Expect",
            "From",
            "Host",
            "If-Match",
            "If-Modified-Since",
            "If-None-Match",
            "If-Range",
            "If-Unmodified-Since",
            "Max-Forwards",
            "Referer",
            "TE",
            "User-Agent",
            //Response headers
            "Accept-Ranges",
            "Age",
            "Allow",
            "ETag",
            "Location",
            "Retry-After",
            "Server",
            "Set-Cookie2",
            "Set-Cookie",
            "Vary",
            "Www-Authenticate",
            // General
            "Cache-Control",
            "Connection",
            "Date",
            "Pragma",
            "Trailer",
            "Transfer-Encoding",
            "Upgrade",
            "Via",
            "Warning",
        };

        public static JObject FilterHeaders(this NameValueCollection self)
        {
            var metadata = new JObject();
            foreach (var header in self.AllKeys)
            {
                if (HeadersToIgnore.Contains(header))
                    continue;
                var values = self.GetValues(header);
                if (values.Length == 1)
                    metadata.Add(header, new JValue(values[0]));
                else
                    metadata.Add(header, new JArray(values.Select(x => new JValue(x))));
            }
            return metadata;
        }

        public static JObject ReadJson(this HttpListenerContext context)
        {
            using (var streamReader = new StreamReader(context.Request.InputStream))
            using (var jsonReader = new JsonTextReader(streamReader))
                return JObject.Load(jsonReader);
        }

        public static JArray ReadJsonArray(this HttpListenerContext context)
        {
            using (var streamReader = new StreamReader(context.Request.InputStream))
            using (var jsonReader = new JsonTextReader(streamReader))
                return JArray.Load(jsonReader);
        }

        public static string ReadString(this HttpListenerContext context)
        {
            using (var streamReader = new StreamReader(context.Request.InputStream))
                return streamReader.ReadToEnd();
        }

        public static void WriteJson(this HttpListenerContext context, object obj)
        {
            context.Response.Headers["Content-Type"] = "application/json; charset=utf-8";
            var streamWriter = new StreamWriter(context.Response.OutputStream);
            new JsonSerializer
            {
                Converters = {new JsonToJsonConverter()}
            }.Serialize(streamWriter, obj);
            streamWriter.Flush();
        }

        public static void WriteJson(this HttpListenerContext context, JToken obj)
        {
            context.Response.Headers["Content-Type"] = "application/json; charset=utf-8";
            var streamWriter = new StreamWriter(context.Response.OutputStream);
            var jsonTextWriter = new JsonTextWriter(streamWriter);
            obj.WriteTo(jsonTextWriter);
            jsonTextWriter.Flush();
            streamWriter.Flush();
        }

        public static void WriteData(this HttpListenerContext context, byte[] data, JObject headers, Guid etag)
        {
            foreach (var header in headers.Properties())
            {
                if (header.Name.StartsWith("@"))
                    continue;
                context.Response.Headers[header.Name] = StringQuotesIfNeeded(header.Value.ToString());
            }
            context.Response.Headers["ETag"] = etag.ToString();
            context.Response.ContentLength64 = data.Length;
            context.Response.OutputStream.Write(data, 0, data.Length);
            context.Response.OutputStream.Flush();
        }

        private static string StringQuotesIfNeeded(string str)
        {
            if (str.StartsWith("\"") && str.EndsWith("\""))
                return str.Substring(1, str.Length - 2);
            return str;
        }

        public static void SetStatusToDeleted(this HttpListenerContext context)
        {
            context.Response.StatusCode = 204;
            context.Response.StatusDescription = "No Content";
        }

        public static void SetStatusToCreated(this HttpListenerContext context, string location)
        {
            context.Response.StatusCode = 201;
            context.Response.StatusDescription = "Created";
            context.Response.Headers["Location"] = location;
        }

        public static void SetStatusToNotFound(this HttpListenerContext context)
        {
            context.Response.StatusCode = 404;
            context.Response.StatusDescription = "Not Found";
        }

        public static void SetStatusToBadRequest(this HttpListenerContext context)
        {
            context.Response.StatusCode = 400;
            context.Response.StatusDescription = "Bad Request";
        }

        public static void Write(this HttpListenerContext context, string str)
        {
            var sw = new StreamWriter(context.Response.OutputStream);
            sw.Write(str);
            sw.Flush();
        }

        /// <summary>
        ///   Reads the entire request buffer to memory and
        ///   return it as a byte array.
        /// </summary>
        public static byte[] ReadData(this Stream steram)
        {
            var list = new List<byte[]>();
            const int defaultBufferSize = 1024*16;
            var buffer = new byte[defaultBufferSize];
            var offset = 0;
            int read;
            while ((read = steram.Read(buffer, offset, buffer.Length - offset)) != 0)
            {
                offset += read;
                if (offset == buffer.Length)
                {
                    list.Add(buffer);
                    buffer = new byte[defaultBufferSize];
                    offset = 0;
                }
            }
            var totalSize = list.Sum(x => x.Length) + offset;
            var result = new byte[totalSize];
            var resultOffset = 0;
            foreach (var partial in list)
            {
                Buffer.BlockCopy(partial, 0, result, resultOffset, partial.Length);
                resultOffset += partial.Length;
            }
            Buffer.BlockCopy(buffer, 0, result, resultOffset, offset);
            return result;
        }

        public static int GetStart(this HttpListenerContext context)
        {
            int start;
            int.TryParse(context.Request.QueryString["start"], out start);
            return start;
        }

        public static int GetPageSize(this HttpListenerContext context)
        {
            int pageSize;
            int.TryParse(context.Request.QueryString["pageSize"], out pageSize);
            if (pageSize == 0)
                pageSize = 25;
            if (pageSize > 1024)
                pageSize = 1024;
            return pageSize;
        }

        public static Guid? GetEtag(this HttpListenerContext context)
        {
            var etagAsString = context.Request.Headers["If-Match"];
            if (etagAsString != null)
            {
                try
                {
                    return new Guid(etagAsString);
                }
                catch
                {
                    throw new BadRequestException("Could not parse If-Match header as Guid");
                }
            }
            return null;
        }

        #region Nested type: JsonToJsonConverter

        public class JsonToJsonConverter : JsonConverter
        {
            public override
                void WriteJson
                (JsonWriter writer, object value)
            {
                ((JObject) value).WriteTo(writer);
            }

            public override
                object ReadJson
                (JsonReader reader, Type objectType)
            {
                throw new NotImplementedException();
            }

            public override
                bool CanConvert
                (Type
                     objectType)
            {
                return objectType == typeof (JObject);
            }
        }

        #endregion
    }
}