// -----------------------------------------------------------------------
//  <copyright file="Jalchr.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Imports.Newtonsoft.Json;
using Xunit;

namespace SlowTests.SlowTests.MailingList
{
    public class Jalchr : RavenTestBase
    {
        [Fact]
        public void CanGetIdWithCorrectCase()
        {
            using (var store = GetDocumentStore())
            {
                store.Conventions.CustomizeJsonSerializer = serializer =>
                               serializer.ReferenceLoopHandling = ReferenceLoopHandling.Ignore;
                store.Conventions.FindFullDocumentKeyFromNonStringIdentifier = (id, type, allowNull) => id.ToString();

                new Agency_Entity().Execute(store);
                new Agency_EntityTransformer().Execute(store);

                var code = "code";

                using (var session = store.OpenSession())
                {
                    var agency = new AgencyDb();
                    agency.Id = Guid.NewGuid();
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
                    var query = session.Query<AgencyDb, Agency_Entity>().Customize(x => x.WaitForNonStaleResultsAsOfLastWrite());
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
                store.Conventions.FindFullDocumentKeyFromNonStringIdentifier = (id, type, allowNull) => id.ToString();

                new Agency_Entity().Execute(store);
                new AgencyTransformer().Execute(store);

                var code = "code";

                using (var session = store.OpenSession())
                {
                    var agency = new AgencyDb();
                    agency.Id = Guid.NewGuid();
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
                        .Customize(x => x.WaitForNonStaleResultsAsOfLastWrite());
                    var agency = Queryable.Where(query, x => x.Code == code)
                        .SingleOrDefault();


                    Assert.NotNull(agency);
                    Assert.True(agency.Countries[0].Agency.Code == agency.Code);
                }
            }
        }

        class AgencyDb
        {
            public Guid Id { get; set; }
            public string Name { get; set; }
            public string Code { get; set; }
            public AgencyCountryDb[] Countries { get; set; }
        }

        class AgencyCountryDb
        {
            public string Country { get; set; }
            public Guid AgencyId { get; set; }
        }

        class Agency
        {
            private List<AgencyCountry> _countries;

            public Guid Id { get; set; }
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

        class AgencyCountry
        {
            public string Country { get; set; }
            public Agency Agency { get; set; }
        }

        class Agency_Entity : AbstractIndexCreationTask<AgencyDb>
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

        class Agency_EntityTransformer : AbstractTransformerCreationTask<AgencyDb>
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

        class Agency_Entity2 : AbstractIndexCreationTask<AgencyDb>
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

        class AgencyTransformer : AbstractTransformerCreationTask<AgencyDb>
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
