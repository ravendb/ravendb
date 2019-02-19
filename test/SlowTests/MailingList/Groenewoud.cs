// -----------------------------------------------------------------------
//  <copyright file="Groenewoud.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers.Collation.Cultures;
using Xunit;

namespace SlowTests.MailingList
{
    public class Groenewoud : RavenTestBase
    {
        private struct ZipCityStateCountry
        {
            public string ZipCode { get; set; }

            public string City { get; set; }

            public string StateCode { get; set; }

            public string CountryCode { get; set; }

            public override string ToString()
            {
                return string.Format("{0} {1} {2} {3}", CountryCode, StateCode, ZipCode, City);
            }
        }

        private class ZC_CountryCityStateCodeACIndex : AbstractIndexCreationTask<ZipCityStateCountry,
            ZC_CountryCityStateCodeACIndex.Result>
        {
            public class Result
            {
#pragma warning disable 649
                public string City;
                public string CountryCode;
                public string StateCode;
                public string CityOrder;
#pragma warning restore 649
            }

            public ZC_CountryCityStateCodeACIndex()
            {
                Map = sEnum => sEnum
                                   .Select(t => new { t.CountryCode, CityOrder = t.City, t.City, t.StateCode });

                Reduce = results => results
                                        .GroupBy(r => new { r.City, r.CountryCode, r.StateCode })
                                        .Select(g => new { g.Key.City, CityOrder = g.Key.City, g.Key.CountryCode, g.Key.StateCode });

                //Bug Collation - following resolves in empty results
                //Without it, {ä ö ü} are analyzed wrongly and are placed at the end of the alphabet
                Analyzers.Add(x => x.CityOrder, typeof(DeCollationAnalyzer).AssemblyQualifiedName);
            }
        }

        [Fact]
        public void CanSortInGerman()
        {
            using (var store = GetDocumentStore())
            {
                new ZC_CountryCityStateCodeACIndex().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new ZipCityStateCountry { CountryCode = "CH", StateCode = "BE", City = "Zauggenried", ZipCode = "a" });
                    session.Store(new ZipCityStateCountry { CountryCode = "CH", StateCode = "BE", City = "Züberwangen", ZipCode = "b" });
                    session.Store(new ZipCityStateCountry { CountryCode = "CH", StateCode = "BE", City = "Zénauva", ZipCode = "c" });
                    session.Store(new ZipCityStateCountry { CountryCode = "CH", StateCode = "BE", City = "Zäziwil", ZipCode = "d" });
                    session.Store(new ZipCityStateCountry { CountryCode = "CH", StateCode = "BE", City = "Zwingen", ZipCode = "e" });
                    session.Store(new ZipCityStateCountry { CountryCode = "CH", StateCode = "BE", City = "Zwillikon", ZipCode = "f" });
                    session.Store(new ZipCityStateCountry { CountryCode = "CH", StateCode = "BE", City = "Zimmerwald", ZipCode = "g" });
                    session.Store(new ZipCityStateCountry { CountryCode = "CH", StateCode = "BE", City = "Zürich 1 Sihlpost", ZipCode = "h" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    QueryStatistics stats;
                    var qRoot =
                        session.Query<ZC_CountryCityStateCodeACIndex.Result, ZC_CountryCityStateCodeACIndex>()
                            .OrderBy(ci => ci.CityOrder)
                            .Statistics(out stats)
                            .Customize(x => x.WaitForNonStaleResults())
                            .Where(ci => ci.CountryCode == "CH" && ci.City.StartsWith("Z"))
                            .As<ZipCityStateCountry>()
                            .ToList();

                    Assert.Equal(8, qRoot.Count);
                    AssertOrder(qRoot);
                }
            }
        }

        private void AssertOrder(List<ZipCityStateCountry> list1)
        {
            for (int i = 0; i < list1.Count - 1; i++)
            {
                Assert.True(string.Compare(list1[i].City, list1[i + 1].City, false, CultureInfo.GetCultureInfo("de-DE")) < 0);
            }
        }
    }
}
