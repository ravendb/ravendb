namespace Raven.Management.Client.Silverlight.Common.Mappers
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Net;
    using Abstractions.Data;
    using Client;
    using Database.Data;
    using Exceptions;
    using Http.Extensions;
    using Newtonsoft.Json.Bson;
    using Newtonsoft.Json.Linq;

    /// <summary>
    /// 
    /// </summary>
    public class AttachmentMapper
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="request"></param>
        /// <param name="result"></param>
        /// <param name="statusCode"></param>
        /// <param name="exception"></param>
        /// <returns></returns>
        public Attachment Map(string key, HttpWebRequest request, IAsyncResult result, out HttpStatusCode statusCode,
                              out Exception exception)
        {
            try
            {
                var response = request.EndGetResponse(result) as HttpWebResponse;
                Stream stream = response.GetResponseStream();

                statusCode = response.StatusCode;
                exception = null;

                var headers = new NameValueCollection();
                foreach (string headerKey in response.Headers.AllKeys)
                {
                    headers.Add(headerKey, response.Headers[key]);
                }

                return new Attachment
                           {
                               Data = stream.ReadData(),
                               Etag = new Guid(response.Headers["ETag"]),
                               Metadata = headers.FilterHeaders(isServerDocument: false)
                           };
            }
            catch (WebException ex)
            {
                var httpWebResponse = ex.Response as HttpWebResponse;
                if (httpWebResponse == null)
                {
                    throw;
                }

                statusCode = httpWebResponse.StatusCode;
                exception = AsyncServerClient.ExtractException(httpWebResponse);

                if (httpWebResponse.StatusCode == HttpStatusCode.Conflict)
                {
                    JObject conflictsDoc = JObject.Load(new BsonReader(httpWebResponse.GetResponseStream()));
                    string[] conflictIds =
                        conflictsDoc.Value<JArray>("Conflicts").Select(x => x.Value<string>()).ToArray();

                    throw new ConflictException("Conflict detected on " + key +
                                                ", conflict must be resolved before the attachment will be accessible")
                              {
                                  ConflictedVersionIds = conflictIds
                              };
                }

                if (httpWebResponse.StatusCode == HttpStatusCode.NotFound)
                {
                    return null;
                }

                throw;
            }
        }
    }
}