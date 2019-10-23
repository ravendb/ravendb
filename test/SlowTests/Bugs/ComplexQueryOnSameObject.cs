using System;
using System.Collections.Generic;
using FastTests;
using Xunit;
using System.Net.Http;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class ComplexQueryOnSameObject : RavenTestBase
    {
        public ComplexQueryOnSameObject(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task WillSucceed()
        {
            using(GetDocumentStore())
            {
                var client = new HttpClient();
                var uri = new Uri(
                    Server.WebUrl + "/indexes/dynamic/AdRequests?query=-Impressions%252CClick%253A%255B%255BNULL_VALUE%255D%255D%2520AND%2520Impressions%252CClick%253A*%2520AND%2520Impressions%252CClick.ClickTime%253A%255B20110205142325841%2520TO%2520NULL%255D&start=0&pageSize=128&aggregation=None");
                await client.GetAsync(uri);
            }
        }

        public class AdRequest
        {
            public IEnumerable<Impression> Impressions { get; set; }
        }

        public class Impression
        {
            public Click Click { get; set; }
        }

        public class Click
        {
            public DateTime ClickTime { get; set; }
        }
    }

}
