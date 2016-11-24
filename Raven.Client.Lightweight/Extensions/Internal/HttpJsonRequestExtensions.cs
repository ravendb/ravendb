using System;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.Extensions
{
    public static class HttpJsonRequestExtensions
    {
        public async static Task AssertNotFailingResponse(this HttpResponseMessage response)
        {
            if (response.IsSuccessStatusCode)
                return;

            using (var sr = new StreamReader(await response.GetResponseStreamWithHttpDecompression().ConfigureAwait(false)))
            {
                var readToEnd = sr.ReadToEnd();

                if (string.IsNullOrWhiteSpace(readToEnd))
                    throw ErrorResponseException.FromResponseMessage(response);

                RavenJObject ravenJObject;
                try
                {
                    ravenJObject = RavenJObject.Parse(readToEnd);
                }
                catch (Exception e)
                {
                    throw new ErrorResponseException(response, readToEnd, e);
                }

                if (ravenJObject.ContainsKey("Error"))
                {
                    var sb = new StringBuilder();
                    foreach (var prop in ravenJObject)
                    {
                        if (prop.Key == "Error")
                            continue;

                        sb.Append(prop.Key).Append(": ").AppendLine(prop.Value.ToString(Formatting.Indented));
                    }

                    if (sb.Length > 0)
                        sb.AppendLine();
                    sb.Append(ravenJObject.Value<string>("Error"));

                    throw new ErrorResponseException(response, sb.ToString(), readToEnd);
                }

                throw new ErrorResponseException(response, readToEnd);
            }
        }
    }
}
