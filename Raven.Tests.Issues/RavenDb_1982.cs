using Raven.Client.Indexes;
using Raven.Tests.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDb_1982 : RavenTest
    {

        public class Property
        {
            public string Id { get; set; }
            public string Name { get; set; }

            public string CityId { get; set; }
        }

        public class City
        {
            public string Id { get; set; }
            public string Name { get; set; }

            public string RegionId { get; set; }
            public string CountryId { get; set; }

        }

        public class Region
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string CountryId { get; set; }
        }

        public class Country
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        public class PropertyIndex : AbstractIndexCreationTask<Property>
        {
            public PropertyIndex()
            {
                Map = docs => from doc in docs
                              select new
                              {

                              };
            }
        }

        public class PropertyTransformer : AbstractTransformerCreationTask<Property>
        {
            public PropertyTransformer()
            {
                TransformResults = docs => from doc in docs
                                           let city = Include<City>(doc.CityId)
                                           let _ = Include(city.RegionId)
                                           let __ = Include(doc.Id)
                                           select new { };
            }
        }

        [Fact]
        public void CanIncludeUsingTransformerForLoad()
        {
            using (var store = NewDocumentStore())
            {
                new PropertyIndex().Execute(store);
                new PropertyTransformer().Execute(store);

                Property property;
                using (var session = store.OpenSession())
                {
                    var country = new Country
                    {
                        Name = "England"
                    };

                    session.Store(country);

                    var region = new Region
                    {
                        Name = "South",
                        CountryId = country.Id
                    };
                    session.Store(region);

                    var city = new City
                    {
                        Name = "London",
                        RegionId = region.Id,
                        CountryId = country.Id,
                    };

                    session.Store(city);

                    property = new Property
                    {
                        Name = "Sample Property",
                        CityId = city.Id
                    };
                    session.Store(property);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Load<PropertyTransformer, object>(property.Id);
                    var before = session.Advanced.NumberOfRequests;
                    var result = session.Load<Property>(property.Id);
                     Assert.True(before == 1); //The .Load<> Call
                     Assert.True(session.Advanced.IsLoaded(result.CityId));
                    var city = session.Load<City>(result.CityId);
                    session.Load<Region>(city.RegionId);

                    Assert.Equal(before, session.Advanced.NumberOfRequests);
                  
                }
            }
        }

        [Fact]
        public void CanIncludeUsingTransformerForIndex()
        {
            using (var store = NewDocumentStore())
            {
                new PropertyIndex().Execute(store);
                new PropertyTransformer().Execute(store);

                Property property;
                using (var session = store.OpenSession())
                {
                    var country = new Country
                    {
                        Name = "England"
                    };

                    session.Store(country);

                    var region = new Region
                    {
                        Name = "South",
                        CountryId = country.Id
                    };
                    session.Store(region);

                    var city = new City
                    {
                        Name = "London",
                        RegionId = region.Id,
                        CountryId = country.Id,
                    };

                    session.Store(city);

                    property = new Property
                    {
                        Name = "Sample Property",
                        CityId = city.Id
                    };
                    session.Store(property);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Query<Property>()
                        .Customize(x=>x.WaitForNonStaleResults())
                        .TransformWith<PropertyTransformer, object>()
                        .ToList();
                    var before = session.Advanced.NumberOfRequests;
                    var result = session.Load<Property>(property.Id);
                    var city = session.Load<City>(result.CityId);
                    session.Load<Region>(city.RegionId);

                    Assert.Equal(before, session.Advanced.NumberOfRequests);
                }
            }
        }
    }
}
