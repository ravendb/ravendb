// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1553.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net;
using Raven.Client.Document;
using Raven.Tests.Common.Attributes;
using Raven.Tests.Common.Util;

using Xunit;

namespace Raven.Tests.Issues
{

    public class RavenDB_1553 : IisExpressTestClient
    {
        [IISExpressInstalledFact]
        public void ShouldRejectInvalidCharactersOnIIS()
        {
            using (NewDocumentStore())
            {
                using (var database = new DocumentStore { Url = "http://localhost:8084" })
                {
                    database.Initialize();
                    DoTest("http://localhost:8084");
                }
            }
        }

        public static void AssertResponseCode(HttpStatusCode expectedResponseCode, string url)
        {
            try
            {
                var request = WebRequest.Create(url);
                request.GetResponse();
                throw new InvalidOperationException("GetResponse should throw.");
            }
            catch (WebException e)
            {
                var httpException = (HttpWebResponse)e.Response;
                Assert.Equal(expectedResponseCode, httpException.StatusCode);
            }
        }

        public static void DoTest(string baseUrl)
        {
            AssertResponseCode(HttpStatusCode.NotFound, baseUrl + "/docs/a**bb");
            AssertResponseCode(HttpStatusCode.BadRequest, baseUrl + "/docs/a%3fb");

            foreach (var illegalCharacter in new[] { '<', '>', '%', '&', ':' })
            {
                var url = baseUrl + "/docs/a" + illegalCharacter;
                AssertResponseCode(HttpStatusCode.BadRequest, url);
            }
        }
    }

    
}