using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Connection;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.TimeSeries;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.TimeSeries
{
    public partial class TimeSeriesStore
    {
        public class TimeSeriesStoreAdminOperations
        {
            private readonly TimeSeriesStore parent;
            
            internal TimeSeriesStoreAdminOperations(TimeSeriesStore parent)
            {
                this.parent = parent;
            }

            /// <summary>
            /// Create new time series on the server.
            /// </summary>
            /// <param name="timeSeriesDocument">Settings for the time series. If null, default settings will be used, and the name specified in the client ctor will be used</param>
            /// <param name="shouldUpdateIfExists">Indicates if time series should be updated if they exist.</param>
            /// <param name="credentials">Credentials used for this operation.</param>
            /// <param name="token">Cancellation token used for this operation.</param>
            public async Task<TimeSeriesStore> CreateTimeSeriesAsync(TimeSeriesDocument timeSeriesDocument, bool shouldUpdateIfExists = false, OperationCredentials credentials = null, CancellationToken token = default(CancellationToken))
            {
                if (timeSeriesDocument == null)
                    throw new ArgumentNullException("timeSeriesDocument");

                parent.AssertInitialized();

                var timeSeriesName = timeSeriesDocument.Id.Replace(Constants.TimeSeries.Prefix, "");
                var requestUri = parent.Url + "admin/ts/" + timeSeriesName;
                if (shouldUpdateIfExists)
                    requestUri += "?update=true";

                using (var request = parent.CreateHttpJsonRequest(requestUri, HttpMethods.Put))
                {
                    try
                    {
                        await request.WriteAsync(RavenJObject.FromObject(timeSeriesDocument)).WithCancellation(token).ConfigureAwait(false);
                    }
                    catch (ErrorResponseException e)
                    {
                        if (e.StatusCode == HttpStatusCode.Conflict)
                            throw new InvalidOperationException("Cannot create time series with the name '" + timeSeriesName + "' because it already exists. Use CreateOrUpdateTimeSeriesAsync in case you want to update an existing time series", e);

                        throw;
                    }
                }

                return new TimeSeriesStore
                {
                    Name = timeSeriesName,
                    Url = parent.Url,
                    Credentials = credentials ?? parent.Credentials
                };
            }

            public async Task DeleteTimeSeriesAsync(string timeSeriesName, bool hardDelete = false, CancellationToken token = default(CancellationToken))
            {
                parent.AssertInitialized();

                var requestUriString = String.Format("{0}/admin/ts/{1}?hard-delete={2}", parent.Url, timeSeriesName, hardDelete);

                using (var request = parent.CreateHttpJsonRequest(requestUriString, HttpMethods.Delete))
                {
                    try
                    {
                        await request.ExecuteRequestAsync().WithCancellation(token).ConfigureAwait(false);
                    }
                    catch (ErrorResponseException e)
                    {
                        if (e.StatusCode == HttpStatusCode.NotFound)
                            throw new InvalidOperationException(string.Format("Time series with specified name ({0}) doesn't exist", timeSeriesName));
                        throw;
                    }
                }
            }

            public async Task<string[]> GetTimeSeriesNamesAsync(CancellationToken token = default(CancellationToken))
            {
                parent.AssertInitialized();

                using (var request = parent.CreateHttpJsonRequest(parent.Url + "ts", HttpMethods.Get))
                {
                    var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                    return response.ToObject<string[]>(parent.JsonSerializer);
                }
            }
        }
    }
}
