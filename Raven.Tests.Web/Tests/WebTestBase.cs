// -----------------------------------------------------------------------
//  <copyright file="WebTestBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;

using Newtonsoft.Json;

using Raven.Tests.Web.Models;

using Xunit;

namespace Raven.Tests.Web.Tests
{
    public abstract class WebTestBase : IUseFixture<WebTestFixture>
    {
        private readonly HttpClient client;

        protected string Url { get; private set; }

        protected WebTestBase()
        {
            client = new HttpClient
                     {
                         Timeout = TimeSpan.FromSeconds(20)
                     };
        }

        protected async Task TestControllerAsync(string controllerName)
        {
            var methods = await GetControllerMethodsAsync(controllerName);
            Assert.True(methods.Count > 0);

            foreach (var method in methods)
            {
                await TestControllerMethodAsync(method);
            }
        }

        private async Task TestControllerMethodAsync(ApiControllerMethod method)
        {
            Console.Write("Testing " + method.Route);

            var request = new HttpRequestMessage(new HttpMethod(method.Method), Url + "/" + method.Route);
            var response = await client.SendAsync(request);
            await HandleErrorsIfNecessaryAsync(response);

            Console.Write(" OK");
            Console.WriteLine();
        }

        private async Task<IList<ApiControllerMethod>> GetControllerMethodsAsync(string controllerName)
        {
            var response = await client.GetAsync(Url + "/api/" + controllerName + "/methods");

            if (response.IsSuccessStatusCode == false)
            {
                string content = null;
                try
                {
                    content = await response.Content.ReadAsStringAsync();
                }
                catch (Exception)
                {
                }

                throw new InvalidOperationException(string.Format("Failed to retrieve methods for '{0}'. Status: {1}. Response: {2}", controllerName, response.StatusCode, content));
            }

            var contentAsString = await response.Content.ReadAsStringAsync();
            return JsonConvert.DeserializeObject<List<ApiControllerMethod>>(contentAsString);
        }

        private async Task HandleErrorsIfNecessaryAsync(HttpResponseMessage response)
        {
            if (response.IsSuccessStatusCode)
                return;

            string content = null;
            try
            {
                content = await response.Content.ReadAsStringAsync();
            }
            catch (Exception)
            {
            }

            throw new InvalidOperationException(string.Format("Request failed. Status: {0}. Response: {1}.", response, content));
        }

        public void SetFixture(WebTestFixture data)
        {
            Url = data.Url;
        }
    }
}
