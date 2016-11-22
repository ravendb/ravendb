// -----------------------------------------------------------------------
//  <copyright file="JsonContent.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Abstractions.Connection
{
    public class JsonContent : HttpContent
    {
        private static readonly Encoding DefaultEncoding = new UTF8Encoding(false);

        public JsonContent(RavenJToken data = null)
        {
            Data = data;
            if (data != null)
            {
                Headers.ContentType = string.IsNullOrEmpty(Jsonp) ?
                    new MediaTypeHeaderValue("application/json") { CharSet = "utf-8" } :
                    new MediaTypeHeaderValue("application/javascript") { CharSet = "utf-8" };
            }
        }

        public RavenJToken Data { get; set; }

        public string Jsonp { get; set; }

        public bool IsOutputHumanReadable { get; set; }

        protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
        {
            if (HasNoData())
                return new CompletedTask<bool>(true);

            using (var undisposableStream = new UndisposableStream(stream))
            using (var bufferedStream = new BufferedStream(undisposableStream))
            {
                var streamToUse = bufferedStream;
                var writer = new StreamWriter(streamToUse, DefaultEncoding);
                if (string.IsNullOrEmpty(Jsonp) == false)
                {
                    writer.Write(Jsonp);
                    writer.Write("(");
                }

                Data.WriteTo(new JsonTextWriter(writer)
                {
                    Formatting = IsOutputHumanReadable ? Formatting.Indented : Formatting.None,
                }, Default.Converters);

                if (string.IsNullOrEmpty(Jsonp) == false)
                    writer.Write(")");

                writer.Flush();
            }

            return new CompletedTask<bool>(true);
        }

        private bool HasNoData()
        {
            return Data == null || Data.Type == JTokenType.Null;
        }

        protected override bool TryComputeLength(out long length)
        {
            var hasNoData = HasNoData();
            length = hasNoData ? 0 : -1;
            return hasNoData;
        }

    }
}
