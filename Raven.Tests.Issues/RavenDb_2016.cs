using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDb_2016 : RavenTestBase
    {
        private Property property;

        [Fact]
        public void CanIndexAndLuceneQueryWithSetResultTransformer()
        {
            using (var store = NewDocumentStore())
            {
                FillDb(store);

                //Lucene SetResultTransformer
                using (var session = store.OpenSession())
                {
                    var result = session.Advanced.LuceneQuery<Property, PropertyIndex>()
                        .WaitForNonStaleResultsAsOfNow()
                        .SetResultTransformer(typeof(PropertyTransformer).Name)
                        .FirstOrDefault();

                    Assert.True(result.Name == null);
                }

            


            }
        }
         [Fact]
        public void CanIndexAndQueryWithTransformWith()
        {
            using (var store = NewDocumentStore())
            {
                FillDb(store);

               

                //Query TransformWith
                using (var session = store.OpenSession())
                {
                    var result = session.Query<Property, PropertyIndex>()
                        .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                        .TransformWith<PropertyTransformer, Property>()
                        .FirstOrDefault();

                    Assert.True(result != null && result.Name == null);
                }

     

            }
        }
         [Fact]
        public void CanIndexAndQueryWithCustomizeSetResultTransformer()
        {
            using (var store = NewDocumentStore())
            {
                FillDb(store);

               

                //Query with Customize SetResultTransformer
                using (var session = store.OpenSession())
                {
                    var result = session.Query<Property, PropertyIndex>()
                        .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                        .Customize(x =>
                        {
                             var luceneQuery = (IDocumentQuery<Property>)x;
                             luceneQuery.SetResultTransformer(typeof(PropertyTransformer).Name);
                        }).FirstOrDefault();


                    Assert.True(result != null && result.Name == null);
                    ;
                }


            }
        }

        private void FillDb(EmbeddableDocumentStore store)
        {
            new PropertyIndex().Execute(store);
            new PropertyTransformer().Execute(store);

            using (var session = store.OpenSession())
            {
                var country = new CountryD
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
        }
    }

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

    public class CountryD
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
                                       let loadedCity = LoadDocument<City>(doc.CityId)
                                       let _ = Include(loadedCity.RegionId)
                                       select new
                                       {
                                           doc.CityId,
                                           RegionId = loadedCity.RegionId,
                                           CountryId = loadedCity.CountryId,
                                       };
        }
    }
}

