// -----------------------------------------------------------------------
//  <copyright file="SingleAuthTokenRetriever.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Specialized;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Connection.Implementation;
using Raven.NewClient.Client.Connection.Profiling;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.Util.Auth
{
    internal class SingleAuthTokenRetriever
    {
        private readonly IHoldProfilingInformation profilingInfo;
        private readonly HttpJsonRequestFactory factory;
        private readonly ConventionBase convention;
        private readonly NameValueCollection operationHeaders;
        private readonly OperationMetadata operationMetadata;

        public SingleAuthTokenRetriever(IHoldProfilingInformation profilingInfo, HttpJsonRequestFactory factory, ConventionBase convention, NameValueCollection operationHeaders, OperationMetadata operationMetadata)
        {
            this.profilingInfo = profilingInfo;
            this.factory = factory;
            this.convention = convention;
            this.operationHeaders = operationHeaders;
            this.operationMetadata = operationMetadata;
        }

        internal async Task<string> GetToken()
        {
            using (var request = CreateRequestParams(operationMetadata, "/singleAuthToken", HttpMethod.Get, disableRequestCompression: true))
            {
                var response = await request.ReadResponseJsonAsync().ConfigureAwait(false);
                return response.Value<string>("Token");
            }
        }

        internal async Task<string> ValidateThatWeCanUseToken(string token)
        {
            using (var request = CreateRequestParams(operationMetadata, "/singleAuthToken", HttpMethod.Get, disableRequestCompression: true, disableAuthentication: true))
            {
                request.AddOperationHeader("Single-Use-Auth-Token", token);
                var result = await request.ReadResponseJsonAsync().ConfigureAwait(false);
                return result.Value<string>("Token");
            }
        }

        private HttpJsonRequest CreateRequestParams(OperationMetadata operationMetadata, string requestUrl, HttpMethod method, bool disableRequestCompression = false, bool disableAuthentication = false)
        {
            var createHttpJsonRequestParams = new CreateHttpJsonRequestParams(profilingInfo, operationMetadata.Url + requestUrl, method, operationMetadata.Credentials, convention)
                .AddOperationHeaders(operationHeaders);

            createHttpJsonRequestParams.DisableRequestCompression = disableRequestCompression;
            createHttpJsonRequestParams.DisableAuthentication = disableAuthentication;

            return factory.CreateHttpJsonRequest(createHttpJsonRequestParams);
        }
    }
}