// -----------------------------------------------------------------------
//  <copyright file="QueryOnItems.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using FastTests;
using Newtonsoft.Json;
using Xunit;
using System.Linq;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;

namespace SlowTests.MailingList
{
    public class QueryOnItems : RavenTestBase
    {
        private class ProviderIdentifier
        {
            public string Provider { get; set; }
            public string Id { get; set; }
        }

        private class FeedItem
        {
            public Guid Id { get; set; }
            public IList<ProviderIdentifier> RelatedProfiles { get; set; }
        }

        [Fact]
        public void ShouldWork()
        {
            var profiles = JsonConvert.DeserializeObject<ProviderIdentifier[]>("[{\"Provider\":0,\"Id\":\"js-\\\"4502453\\\"\"},{\"Provider\":6,\"Id\":\"311862799506\"},{\"Provider\":4,\"Id\":\"groupon\"},{\"Provider\":3,\"Id\":\"175045424\"},{\"Provider\":6,\"Id\":\"176348809057391\"},{\"Provider\":3,\"Id\":\"34686205\"},{\"Provider\":6,\"Id\":\"125988007456412\"},{\"Provider\":6,\"Id\":\"82991127915\"},{\"Provider\":2,\"Id\":\"355611\"},{\"Provider\":4,\"Id\":\"centzy\"},{\"Provider\":6,\"Id\":\"163923827016431\"},{\"Provider\":3,\"Id\":\"301171418\"},{\"Provider\":2,\"Id\":\"2453718\"},{\"Provider\":4,\"Id\":\"coffee-meets-bagel\"},{\"Provider\":2,\"Id\":\"2504677\"},{\"Provider\":4,\"Id\":\"beachmint\"},{\"Provider\":6,\"Id\":\"127540713947214\"},{\"Provider\":4,\"Id\":\"beach-mint\"},{\"Provider\":2,\"Id\":\"1310723\"},{\"Provider\":4,\"Id\":\"babbaco\"},{\"Provider\":6,\"Id\":\"30424541055\"},{\"Provider\":3,\"Id\":\"31930195\"},{\"Provider\":6,\"Id\":\"106229596074522\"},{\"Provider\":6,\"Id\":\"73665029540\"},{\"Provider\":2,\"Id\":\"280683\"},{\"Provider\":4,\"Id\":\"benchprep\"},{\"Provider\":4,\"Id\":\"cloudbot\"},{\"Provider\":3,\"Id\":\"346725079\"},{\"Provider\":6,\"Id\":\"170216073032912\"},{\"Provider\":4,\"Id\":\"lifecrowd\"},{\"Provider\":6,\"Id\":\"188456244507673\"},{\"Provider\":4,\"Id\":\"onswipe\"},{\"Provider\":6,\"Id\":\"100858346620905\"},{\"Provider\":4,\"Id\":\"udemy\"},{\"Provider\":2,\"Id\":\"822535\"},{\"Provider\":3,\"Id\":\"883288452\"},{\"Provider\":6,\"Id\":\"168851749919862\"},{\"Provider\":4,\"Id\":\"sunnybump\"},{\"Provider\":2,\"Id\":\"2785290\"}]");
            using (var documentStore = GetDocumentStore())
            using (var session = documentStore.OpenSession())
            {
                QueryStatistics stats;
                session.Query<FeedItem>()
                       .Customize(x => x.WaitForNonStaleResults())
                       .Statistics(out stats)
                       .Where(fi => fi.RelatedProfiles.Any(rp => rp.In(profiles)))
                       .ToArray();
            }
        }
    }
}
