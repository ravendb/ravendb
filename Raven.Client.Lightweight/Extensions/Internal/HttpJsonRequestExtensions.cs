using System;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;

namespace Raven.Client.Extensions
{
    internal static class HttpJsonRequestExtensions
    {
        internal async static Task AssertNotFailingResponse(this HttpResponseMessage response)
        {
            if (response.IsSuccessStatusCode == false)
            {
                var sb = new StringBuilder()
                    .Append(response.StatusCode)
                    .AppendLine();

                using (var reader = new StreamReader(await response.GetResponseStreamWithHttpDecompression().ConfigureAwait(false)))
                {
                    string line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        sb.AppendLine(line);
                    }
                }
                throw new InvalidOperationException(sb.ToString());
            }
        }

    }
}
