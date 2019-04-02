// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3286.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Net.Http;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3620 : IDisposable
    {
        private readonly HttpClient _client = new HttpClient();

        [Fact]
        public void VerifyLicenseRelatedLinks()
        {
            /*
             * Purpose of this test is to detect unexpected links change on ravendb.net page.
             * 
             * In https://issues.hibernatingrhinos.com/issue/RavenDB-3620 we introduce few links in studio:
             * 
             * - contact us
             * - support
             * - support request
             * - forum
             *
             * When this test fails (and it isn't network failure) please remember to update links in studio (licensingStatus.html file). 
             * 
             * Hopefully by doing this we avoid client frustration in case of broken links. :)
             */
            VerifyLink("http://ravendb.net/contact");
            VerifyLink("https://groups.google.com/forum/#!forum/ravendb");
            VerifyLink("http://ravendb.net/support");
            VerifyLink("http://ravendb.net/support/supportrequest");

        }

        private void VerifyLink(string url)
        {
            var response = _client.GetAsync(url).Result;
            Assert.True(response.IsSuccessStatusCode, url);
        }

        public void Dispose()
        {
            _client.Dispose();
        }
    }
}
