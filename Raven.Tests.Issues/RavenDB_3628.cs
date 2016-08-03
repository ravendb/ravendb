// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3628.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Indexes;
using Raven.Database.Config;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_3628 : RavenTest
    {
        private class City
        {
            public City()
            {
                Districts = new List<District>();
            }

            public string Name { get; set; }

            public List<District> Districts { get; set; }
        }

        private class District
        {
            public string Name { get; set; }

            public int PostalCode { get; set; }
        }

        private class City_ByDistrictNameAndPostalCode : AbstractIndexCreationTask<City, City_ByDistrictNameAndPostalCode.Result>
        {
            public class Result
            {
                public string CityName { get; set; }

                public string[] DistrictNames { get; set; }

                public int[] PostalCodes { get; set; }
            }

            public City_ByDistrictNameAndPostalCode()
            {
                Map = cities => from city in cities
                                let otherCities = LoadDocument<City>(new[] { "cities/1", "cities/2", "cities/3" })
                                select new
                                {
                                    CityName = city.Name,
                                    DistrictNames = otherCities.SelectMany(x => x.Districts).Select(x => x.Name),
                                    PostalCodes = otherCities.SelectMany(x => x.Districts).Select(x => x.PostalCode)
                                };

                Reduce = results => from result in results
                                    group result by result.CityName into g
                                    select new
                                    {
                                        CityName = g.Key,
                                        DistrictNames = g.SelectMany(x => x.DistrictNames).ToArray(),
                                        PostalCodes = g.SelectMany(x => x.PostalCodes).ToArray()
                                    };
            }
        }

        protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
        {
            configuration.Settings["Raven/Esent/MaxVerPages"] = "2";
            configuration.Settings["Raven/Esent/PreferredVerPages"] = "2";
            configuration.Settings[Constants.Voron.MaxScratchBufferSize] = "7";
        }

        [Theory]
        [PropertyData("Storages")]
        public void IfWeHitOutOfMemoryDuringIndexingThenWeShouldDisableIndexAndCreateAnAlert(string requestedStorage)
        {
            using (var store = NewRemoteDocumentStore(requestedStorage: requestedStorage))
            {
                store.DatabaseCommands.Admin.StopIndexing();

                for (int i = 0; i < 3; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        var city = GenerateCity(i);
                        session.Store(city);
                        session.SaveChanges();
                    }
                }

                var index = new City_ByDistrictNameAndPostalCode();
                index.Execute(store);
                store.DatabaseCommands.Admin.StartIndexing();

                var result = SpinWait.SpinUntil(() =>
                {
                    var stats = store.DatabaseCommands.GetStatistics();
                    var indexStats = stats.Indexes.First(x => x.Name == index.IndexName);
                    return indexStats.Priority == IndexingPriority.Disabled;
                }, TimeSpan.FromSeconds(120));

                Assert.True(result);

                result = SpinWait.SpinUntil(() =>
                {
                    var doc = store.DatabaseCommands.Get(Constants.RavenAlerts);
                    return doc != null;
                }, TimeSpan.FromSeconds(10));

                Assert.True(result);

                var alertsJson = store.DatabaseCommands.Get(Constants.RavenAlerts);
                var alerts = alertsJson.DataAsJson.JsonDeserialization<AlertsDocument>() ?? new AlertsDocument();
                var alert = alerts.Alerts.FirstOrDefault(x => x.Title == $"Index '{index.IndexName}' marked as disabled due to out of memory exception");
                Assert.NotNull(alert);
            }
        }

        private static City GenerateCity(int index)
        {
            var cityName = "City";
            var city = new City { Name = cityName };
            for (var i = 0; i < 30000; i++)
            {
                var districtName = cityName + "/District/" + i;
                city.Districts.Add(new District
                {
                    Name = districtName,
                    PostalCode = i
                });
            }

            return city;
        }
    }
}
