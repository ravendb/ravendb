// -----------------------------------------------------------------------
//  <copyright file="Jalchr.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Newtonsoft.Json;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Client.Linq;
using Xunit;

namespace SlowTests.SlowTests.MailingList
{
    public class Jalchr : RavenNewTestBase
    {
        [Fact]
        public void CanGetIdWithCorrectCase()
        {
            using (var store = GetDocumentStore())
            {
                store.Conventions.CustomizeJsonSerializer = serializer =>
                               serializer.ReferenceLoopHandling = ReferenceLoopHandling.Ignore;

                new Agency_Entity().Execute(store);
                new Agency_EntityTransformer().Execute(store);

                var code = "code";

                using (var session = store.OpenSession())
                {
                    var agency = new AgencyDb();
                    agency.Id = Guid.NewGuid().ToString();
                    agency.Name = "my-name";
                    agency.Code = code;
                    var country = new AgencyCountryDb();
                    country.AgencyId = agency.Id;
                    country.Country = "The-Country";
                    agency.Countries = new AgencyCountryDb[] { country };

                    session.Store(agency);
                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    var query = session.Query<AgencyDb, Agency_Entity>().Customize(x => x.WaitForNonStaleResults());
                    var agency = (Queryable.Where(query, x => x.Code == code) as IRavenQueryable<AgencyDb>).TransformWith<Agency_EntityTransformer, Agency>().SingleOrDefault();

                    Assert.NotNull(agency);
                    Assert.True(agency.Countries[0].Agency.Code == agency.Code);
                }
            }
        }

        [Fact]
        public void CanGetIdWithCorrectCaseWithTransforms()
        {
            using (var store = GetDocumentStore())
            {
                store.Conventions.CustomizeJsonSerializer = serializer =>
                               serializer.ReferenceLoopHandling = ReferenceLoopHandling.Ignore;

                new Agency_Entity().Execute(store);
                new AgencyTransformer().Execute(store);

                var code = "code";

                using (var session = store.OpenSession())
                {
                    var agency = new AgencyDb();
                    agency.Id = Guid.NewGuid().ToString();
                    agency.Name = "my-name";
                    agency.Code = code;
                    var country = new AgencyCountryDb();
                    country.AgencyId = agency.Id;
                    country.Country = "The-Country";
                    agency.Countries = new AgencyCountryDb[] { country };

                    session.Store(agency);
                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    var query = session.Query<AgencyDb, Agency_Entity>()
                        .TransformWith<AgencyTransformer, Agency>()
                        .Customize(x => x.WaitForNonStaleResults());
                    var agency = Queryable.Where(query, x => x.Code == code)
                        .SingleOrDefault();


                    Assert.NotNull(agency);
                    Assert.True(agency.Countries[0].Agency.Code == agency.Code);
                }
            }
        }

        private class AgencyDb
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Code { get; set; }
            public AgencyCountryDb[] Countries { get; set; }
        }

        private class AgencyCountryDb
        {
            public string Country { get; set; }
            public string AgencyId { get; set; }
        }

        private class Agency
        {
            private List<AgencyCountry> _countries;

            public string Id { get; set; }
            public string Name { get; set; }
            public string Code { get; set; }


            public AgencyCountry[] Countries
            {
                get
                {
                    if (_countries == null)
                        return new AgencyCountry[0];
                    return _countries.ToArray();
                }
                set
                {
                    _countries = new List<AgencyCountry>(value);
                }
            }
        }

        private class AgencyCountry
        {
            public string Country { get; set; }
            public Agency Agency { get; set; }
        }

        private class Agency_Entity : AbstractIndexCreationTask<AgencyDb>
        {
            public Agency_Entity()
            {
                Map = agencies => from agency in agencies
                                  select new
                                  {
                                      agency.Code,
                                  };
            }
        }

        private class Agency_EntityTransformer : AbstractTransformerCreationTask<AgencyDb>
        {
            public Agency_EntityTransformer()
            {
                TransformResults = agencies => from agency in agencies
                                               select new // AgencyDb
                                               {
                                                   agency.Id,
                                                   agency.Name,
                                                   agency.Code,
                                                   Countries = from com in agency.Countries
                                                               select new // AgencyCountry
                                                               {
                                                                   com.Country,
                                                                   Agency = agency
                                                               }
                                               };
            }
        }

        private class Agency_Entity2 : AbstractIndexCreationTask<AgencyDb>
        {
            public Agency_Entity2()
            {
                Map = agencies => from agency in agencies
                                  select new
                                  {
                                      agency.Code,
                                  };

            }
        }

        private class AgencyTransformer : AbstractTransformerCreationTask<AgencyDb>
        {
            public AgencyTransformer()
            {
                TransformResults = agencies => from agency in agencies
                                               select new // Agency
                                               {
                                                   agency.Id,
                                                   agency.Name,
                                                   agency.Code,
                                                   Countries = from com in agency.Countries
                                                               select new // AgencyCountry
                                                               { com.Country, Agency = agency }
                                               };
            }
        }

    }
}
