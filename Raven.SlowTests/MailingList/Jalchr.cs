// -----------------------------------------------------------------------
//  <copyright file="Jalchr.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Imports.Newtonsoft.Json;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.MailingList
{
    using System.Collections;
    using System.Diagnostics;

    using Raven.Abstractions.Indexing;
    using Raven.Client.Linq;

    public class Jalchr : RavenTest
    {
        [Fact]
        public void CanGetIdWithCorrectCase()
        {
            using (var store = NewDocumentStore())
            {
                store.Conventions.CustomizeJsonSerializer = serializer =>
                               serializer.ReferenceLoopHandling = ReferenceLoopHandling.Ignore;
                store.Conventions.FindFullDocumentKeyFromNonStringIdentifier = (id, type, allowNull) => id.ToString();

                new Agency_Entity().Execute(store);


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
                    var agency = Queryable.Where(query, x => x.Code == code).As<Agency>().SingleOrDefault();

                    Assert.NotNull(agency);
                    Assert.True(agency.Countries[0].Agency.Code == agency.Code);
                }
            }
        }

        [Fact]
        public void CanGetIdWithCorrectCaseWithTransforms()
        {
            using (var store = NewDocumentStore())
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
                get { return _countries.ToArray(); }
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

                TransformResults = (database, agencies) => from agency in agencies
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

    public class Jalchr2 : RavenTest
    {
        [Fact]
        public void Streaming_documents_will_respect_the_sorting_order()
        {
            using (var store = NewDocumentStore())
            {
                store.Conventions.CustomizeJsonSerializer = serializer =>
                               serializer.ReferenceLoopHandling = ReferenceLoopHandling.Ignore;
                store.Conventions.FindFullDocumentKeyFromNonStringIdentifier = (id, type, allowNull) => id.ToString();
                store.Conventions.AllowQueriesOnId = true;

                new User_Entity().Execute(store);

                var iteration = 100;
                var list = new List<User>();
                var now = DateTime.Now.AddYears(-50).Date;
                var start = now;
                for (int k = 0; k < iteration; k++)
                {
                    now = now.AddMonths(k);
                    using (var session = store.OpenSession())
                    {
                        for (int i = 0; i < 672; i++)
                        {
                            var user = new User();
                            user.Id = Guid.NewGuid();
                            user.Name = "User" + ("-" + k + "-" + i);
                            user.CreatedDate = now.AddHours(i);
                            list.Add(user);

                            session.Store(user);
                        }
                        session.SaveChanges();
                    }
                }

                int count;
                // Warm-up
                using (var session = store.OpenSession())
                {
                    RavenQueryStatistics stats;
                    var query = session.Query<User, User_Entity>()
                                       .Customize(x => x.WaitForNonStaleResultsAsOfLastWrite())
                                       .Statistics(out stats);
                    query = query.Where(x => x.CreatedDate >= start.Date);
                    query = query.Where(x => x.CreatedDate <= DateTime.Now.Date);
                    var result = query.OrderBy(x => x.CreatedDate).ToList();

                    Assert.True(result.Count > 0);
                    count = query.Count();
                }

                WaitForIndexing(store);

                var orderedList = list.OrderBy(x => x.CreatedDate).ToList();
                using (var session = store.OpenSession())
                {
                    RavenQueryStatistics stats;
                    var query = session.Query<User, User_Entity>()
                                       .Statistics(out stats);

                    query = query.Where(x => x.CreatedDate >= start.Date);
                    query = query.Where(x => x.CreatedDate <= DateTime.Now.Date);

                    var streamQuery = query
                                    .OrderBy(x => x.CreatedDate)
                                    .As<User>();

                    var enumerator = session.Advanced.Stream(streamQuery);
                    var index = 0;
                    while (enumerator.MoveNext())
                    {
                        if (enumerator.Current.Document.CreatedDate != orderedList[index].CreatedDate)
                            Debugger.Break();

                        Assert.True(enumerator.Current.Document.CreatedDate == orderedList[index].CreatedDate,
                            "Failed at: " + index
                            + ", " + enumerator.Current.Document.CreatedDate.ToString("yyyy-MM-dd hh:mm:ss.sssssss")
                            + " != " + orderedList[index].CreatedDate.ToString("yyyy-MM-dd hh:mm:ss.sssssss")
                            );
                        index++;
                    }
                    Assert.Equal(index, count);
                }
            }
        }
        class User
        {
            public Guid Id { get; set; }
            public string Name { get; set; }
            public DateTime CreatedDate { get; set; }
        }

        class User_Entity : AbstractIndexCreationTask<User>
        {
            public User_Entity()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Id,
                                   user.Name,
                                   user.CreatedDate,
                               };
            }
        }


    }

}