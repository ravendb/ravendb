// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3286.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Net.Http;
using System.Threading.Tasks;

using Raven.Abstractions.Replication;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Metrics;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3620 : IDisposable
    {
        private HttpClient client = new HttpClient();

        [Fact]
        public void VerifyLicenseRelatedLinks()
        {
            /*
             * Purpose of this test is to detect unexpected links change on ravendb.net page.
             * 
             * In http://issues.hibernatingrhinos.com/issue/RavenDB-3620 we introduce few links in studio:
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
            var response = client.GetAsync(url).Result;
            Assert.True(response.IsSuccessStatusCode);
        }

        public void Dispose()
        {
            client.Dispose();
        }
    }
}
