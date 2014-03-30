// -----------------------------------------------------------------------
//  <copyright file="RavenFsWebApiTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Net;
using Raven.Client.RavenFS;

namespace RavenFS.Tests
{
    public class RavenFsWebApiTest : RavenFsTestBase
    {
        protected string WebApiTestName = "RavenFS_WebApi";

        public RavenFsWebApiTest()
        {
            var ravenFsClient = NewClient(fileSystemName: WebApiTestName);

            WebClient = new WebClient()
            {
                BaseAddress = GetServerUrl(false, ravenFsClient.ServerUrl)
            };
        }

        public WebClient WebClient { get; set; }

        protected HttpWebRequest CreateWebRequest(string url)
        {
            return (HttpWebRequest)WebRequest.Create(WebClient.BaseAddress + url);
        }

        public override void Dispose()
        {
            base.Dispose();
            
            if(WebClient != null)
                WebClient.Dispose();
        }

        protected string GetFsUrl(string url)
        {
            if (url.StartsWith("/"))
                url = url.Trim('/');

            return string.Format("/ravenfs/{0}/{1}", WebApiTestName, url);
        }
    }
}