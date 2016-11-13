using System;
using System.Globalization;
using System.Net.Http;
using Raven.Abstractions.Connection;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Connection.Implementation;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.NewClient.Client.TimeSeries.Operations
{
    /// <summary>
    /// implements administration level time series functionality
    /// </summary>
    public abstract class TimeSeriesOperationsBase
    {
        private readonly OperationCredentials credentials;
        private readonly HttpJsonRequestFactory jsonRequestFactory;
        private readonly TimeSeriesConvention timeSeriesConvention;
        protected readonly string ServerUrl;
        protected readonly JsonSerializer JsonSerializer;
        protected readonly string TimeSeriesUrl;
        protected readonly TimeSeriesStore Parent;
        protected readonly string TimeSeriesName;

        protected TimeSeriesOperationsBase(TimeSeriesStore store, string timeSeriesName)
        {
            credentials = store.Credentials;
            jsonRequestFactory = store.JsonRequestFactory;
            ServerUrl = store.Url;
            Parent = store;
            TimeSeriesName = timeSeriesName;
            TimeSeriesUrl = string.Format(CultureInfo.InvariantCulture, "{0}ts/{1}", ServerUrl, timeSeriesName);
            JsonSerializer = store.JsonSerializer;
            timeSeriesConvention = store.TimeSeriesConvention;
        }

        protected HttpJsonRequest CreateHttpJsonRequest(string requestUriString, HttpMethod httpMethod, bool disableRequestCompression = false, bool disableAuthentication = false, TimeSpan? timeout = null)
        {
            CreateHttpJsonRequestParams @params;
            if (timeout.HasValue)
            {
                @params = new CreateHttpJsonRequestParams(null, requestUriString, httpMethod, credentials, timeSeriesConvention)
                {
                    DisableRequestCompression = disableRequestCompression,
                    DisableAuthentication = disableAuthentication,
                    Timeout = timeout.Value
                };
            }
            else
            {
                @params = new CreateHttpJsonRequestParams(null, requestUriString, httpMethod, credentials, timeSeriesConvention)
                {
                    DisableRequestCompression = disableRequestCompression,
                    DisableAuthentication = disableAuthentication,
                };				
            }
            var request = jsonRequestFactory.CreateHttpJsonRequest(@params);
        
            return request;
        }
    }
}
