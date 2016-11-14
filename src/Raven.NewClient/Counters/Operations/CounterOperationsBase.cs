using System;
using System.Globalization;
using System.Net.Http;
using Raven.NewClient.Abstractions.Connection;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Connection.Implementation;

namespace Raven.NewClient.Client.Counters.Operations
{
    /// <summary>
    /// implements administration level counters functionality
    /// </summary>
    public abstract class CounterOperationsBase 
    {
        private readonly OperationCredentials credentials;
        private readonly HttpJsonRequestFactory jsonRequestFactory;
        private readonly CountersConvention countersConvention;
        protected readonly string ServerUrl;
        protected readonly string CounterStorageUrl;

        protected CounterOperationsBase(CounterStore parent, string counterStorageName)
        {
            credentials = parent.Credentials;
            jsonRequestFactory = parent.JsonRequestFactory;
            ServerUrl = parent.Url;
            CounterStorageUrl = string.Format(CultureInfo.InvariantCulture, "{0}/cs/{1}", ServerUrl, counterStorageName);
            countersConvention = parent.CountersConvention;
        }

        protected HttpJsonRequest CreateHttpJsonRequest(string requestUriString, HttpMethod httpMethod, bool disableRequestCompression = false, bool disableAuthentication = false, TimeSpan? timeout = null)
        {
            CreateHttpJsonRequestParams @params;
            if (timeout.HasValue)
            {
                @params = new CreateHttpJsonRequestParams(null, requestUriString, httpMethod, credentials, countersConvention)
                {
                    DisableRequestCompression = disableRequestCompression,
                    DisableAuthentication = disableAuthentication,
                    Timeout = timeout.Value
                };
            }
            else
            {
                @params = new CreateHttpJsonRequestParams(null, requestUriString, httpMethod, credentials, countersConvention)
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
