using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using Kayak;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Linq;

namespace Rhino.DivanDB.Server.Responders
{
    public static class KayakExtensions
    {
        private static readonly HashSet<string> HeadersToIgnore = new HashSet<string>
        {
            "User-Agent",
            "Host",
            "Content-Length"
        };

        public static NameValueCollection ToNameValueCollection(this NameValueDictionary self)
        {
            var nvc = new NameValueCollection();

            foreach (var k in self)
            {
                if(HeadersToIgnore.Contains(k.Name))
                    continue;

                foreach (var val in k.Values)
                {
                    nvc.Add(k.Name, val);
                }
            }

            return nvc;
        }

        public static JObject ReadJson(this KayakContext context)
        {
            using (var streamReader = new StreamReader(context.Request.InputStream))
            using (var jsonReader = new JsonTextReader(streamReader))
                return JObject.Load(jsonReader);
        }

        public static string ReadString(this KayakContext context)
        {
            using (var streamReader = new StreamReader(context.Request.InputStream))
                return streamReader.ReadToEnd();
        }

        public static void WriteJson(this KayakContext context, object obj)
        {
            context.Response.Headers["Content-Type"] = "application/json; charset=utf-8";
            new JsonSerializer
            {
                Converters = {new JsonToJsonConverter()}
            }.Serialize(context.Response.Output, obj);
        }

        public static void WriteJson(this KayakContext context, JToken obj)
        {
            context.Response.Headers["Content-Type"] = "application/json; charset=utf-8";
            obj.WriteTo(new JsonTextWriter(context.Response.Output));
        }

        public static void WriteData(this KayakContext context, byte[] data, NameValueCollection headers)
        {
            foreach (var header in headers.AllKeys)
            {
                context.Response.Headers[header] = headers[header];
            }
            Stream stream = context.Response.GetDirectOutputStream(data.Length);
            stream.Write(data, 0, data.Length);
        }

        public static void SetStatusToDeleted(this KayakContext context)
        {
            context.Response.StatusCode = 204;
            context.Response.ReasonPhrase = "No Content";
        }

        public static void SetStatusToCreated(this KayakContext context, string location)
        {
            context.Response.SetStatusToCreated();
            context.Response.Headers["Location"] = location;
        }


        /// <summary>
        /// Reads the entire request buffer to memory and
        /// return it as a byte array.
        /// </summary>
        public static byte[] ReadData(this KayakContext context)
        {
            var list = new List<byte[]>();
            var inputStream = context.Request.InputStream;
            const int defaultBufferSize = 1024*16;
            var buffer = new byte[defaultBufferSize];
            int offset = 0;
            int read;
            while ((read = inputStream.Read(buffer, offset, buffer.Length - offset)) != 0)
            {
                offset += read;
                if (offset == buffer.Length)
                {
                    list.Add(buffer);
                    buffer = new byte[defaultBufferSize];
                    offset = 0;
                }
            }
            int totalSize = list.Sum(x => x.Length) + offset;
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