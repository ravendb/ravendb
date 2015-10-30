// -----------------------------------------------------------------------
//  <copyright file="SyncSessionController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Web.Http;

using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Tests.Common.Dto;
using Raven.Tests.Web.Models.Transformers;

namespace Raven.Tests.Web.Controllers.Session
{
    public class SyncSessionController : RavenSyncApiController
    {
        [Route("api/sync/session/load1")]
        public HttpResponseMessage Load1()
        {
            Session.Load<Person>("people/1");
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/load2")]
        public HttpResponseMessage Load2()
        {
            Session.Load<Person>("people/1", typeof(PersonTransformer));
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/load3")]
        public HttpResponseMessage Load3()
        {
            Session.Load<Person>(new[] { "people/1", "people/2" });
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/load4")]
        public HttpResponseMessage Load4()
        {
            Session.Load<Person>(new[] { "people/1", "people/2" });
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/load5")]
        public HttpResponseMessage Load5()
        {
            Session.Load<Person>(new[] { "people/1", "people/2" }, typeof(PersonTransformer));
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/load6")]
        public HttpResponseMessage Load6()
        {
            Session.Load<Person>(1);
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/loadWithInclude1")]
        public HttpResponseMessage LoadWithInclude1()
        {
            Session
                .Include("AddressId")
                .Load<Person>("people/1");
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/loadWithInclude2")]
        public HttpResponseMessage LoadWithInclude2()
        {
            Session
                .Include("AddressId")
                .Load<Person>(new[] { "people/1", "people/2" });
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/query")]
        public HttpResponseMessage Query()
        {
            Session
                .Query<Person>()
                .ToList();
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/saveChanges")]
        public HttpResponseMessage SaveChanges()
        {
            Session
                .SaveChanges();
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/store")]
        public HttpResponseMessage Store()
        {
            Session
                .Store(new Person());
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/delete1")]
        public HttpResponseMessage Delete1()
        {
            Session
                .Delete("people/1");
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/delete2")]
        public HttpResponseMessage Delete2()
        {
            Session
                .Delete<Person>(1);
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/clear")]
        public HttpResponseMessage Clear()
        {
            Session
                .Advanced
                .Clear();
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/defer")]
        public HttpResponseMessage Defer()
        {
            Session
                .Advanced
                .Defer(new DeleteCommandData { Key = "keys/1" });
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/documentQuery")]
        public HttpResponseMessage DocumentQuery()
        {
            Session
                .Advanced
                .DocumentQuery<Person>()
                .ToList();
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/eagerly/executeAllPendingLazyOperations")]
        public HttpResponseMessage ExecuteAllPendingLazyOperations()
        {
            Session
                .Advanced
                .Defer(new DeleteCommandData { Key = "keys/1" });

            Session
                .Advanced
                .Eagerly
                .ExecuteAllPendingLazyOperations();
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/evict")]
        public HttpResponseMessage Evict()
        {
            var person = new Person();

            Session
                .Store(person);

            Session
                .Advanced
                .Evict(person);
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/explicitlyVersion")]
        public HttpResponseMessage ExplicitlyVersion()
        {
            var person = new Person();

            Session
                .Store(person);

            Session
                .Advanced
                .ExplicitlyVersion(person);
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/getDocumentId")]
        public HttpResponseMessage GetDocumentId()
        {
            var person = new Person();

            Session
                .Store(person);

            Session
                .Advanced
                .GetDocumentId(person);
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/getDocumentUrl")]
        public HttpResponseMessage GetDocumentUrl()
        {
            var person = new Person();

            Session
                .Store(person);

            Session
                .Advanced
                .GetDocumentUrl(person);
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/getEtagFor")]
        public HttpResponseMessage GetEtagFor()
        {
            var person = new Person();

            Session
                .Store(person);

            Session
                .Advanced
                .GetEtagFor(person);
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/getMetadataFor")]
        public HttpResponseMessage GetMetadataFor()
        {
            var person = new Person();

            Session
                .Store(person);

            Session
                .Advanced
                .GetMetadataFor(person);
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/hasChanged")]
        public HttpResponseMessage HasChanged()
        {
            var person = new Person();

            Session
                .Store(person);

            Session
                .Advanced
                .HasChanged(person);
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/ignoreChangesFor")]
        public HttpResponseMessage IgnoreChangesFor()
        {
            var person = new Person();

            Session
                .Store(person);

            Session
                .Advanced
                .IgnoreChangesFor(person);
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/isLoaded")]
        public HttpResponseMessage IsLoaded()
        {
            var person = new Person();

            Session
                .Store(person);

            Session
                .Advanced
                .IsLoaded("people/1");
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/lazily/loadWithInclude1")]
        public HttpResponseMessage LazyLoadWithInclude1()
        {
            var lazy = Session
                .Advanced
                .Lazily
                .Include("AddressId")
                .Load<Person>("people/1");

            var person = lazy.Value;
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/lazily/loadWithInclude2")]
        public HttpResponseMessage LazyLoadWithInclude2()
        {
            var lazy = Session
                .Advanced
                .Lazily
                .Include("AddressId")
                .Load<Person>(new[] { "people/1", "people/2" });

            var people = lazy.Value;
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/lazily/load1")]
        public HttpResponseMessage LazyLoad1()
        {
            var lazy = Session
                .Advanced
                .Lazily
                .Load<Person>("people/1");

            var person = lazy.Value;
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/lazily/load2")]
        public HttpResponseMessage LazyLoad2()
        {
            var lazy = Session
                .Advanced
                .Lazily
                .Load<Person>(new[] { "people/1", "people/2" });

            var people = lazy.Value;
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/lazily/loadStartingWith")]
        public HttpResponseMessage LazyLoadStartingWith()
        {
            var lazy = Session
                .Advanced
                .Lazily
                .LoadStartingWith<Person>("people/1");

            var people = lazy.Value;
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/lazily/moreLikeThis")]
        public HttpResponseMessage LazyMoreLikeThis()
        {
            var key = Guid.NewGuid().ToString();
            Session.Store(new Person { Id = key });
            Session.SaveChanges();

            SpinWait.SpinUntil(() => DocumentStore.DatabaseCommands.GetStatistics().StaleIndexes.Length == 0);

            var lazy = Session
                .Advanced
                .Lazily
                .MoreLikeThis<dynamic>(new MoreLikeThisQuery
                                      {
                                          IndexName = new RavenDocumentsByEntityName().IndexName,
                                          DocumentId = key
                                      });

            var people = lazy.Value;
            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/loadStartingWith")]
        public HttpResponseMessage LoadStartingWith()
        {
            Session
                .Advanced
                .LoadStartingWith<Person>("people/");

            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/luceneQuery")]
        public HttpResponseMessage LuceneQuery()
        {
            Session
                .Advanced
                .LuceneQuery<Person>()
                .ToList();

            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/markReadOnly")]
        public HttpResponseMessage MarkReadOnly()
        {
            var person = new Person();

            Session
                .Store(person);

            Session
                .Advanced
                .MarkReadOnly(person);

            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/multiFacetedSearch")]
        public HttpResponseMessage MultiFacetedSearch()
        {
            var person = new Person();

            Session
                .Store(person);

            Session
                .Advanced
                .MultiFacetedSearch(new[]
                                    {
                                        new FacetQuery
                                        {
                                            IndexName = new RavenDocumentsByEntityName().IndexName, 
                                            Query = new IndexQuery(), 
                                            Facets = new List<Facet>
                                                     {
                                                         new Facet
                                                         {
                                                             Name = "Facet1", 
                                                             Mode = FacetMode.Default
                                                         }
                                                     }
                                        }
                                    });

            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/refresh")]
        public HttpResponseMessage Refresh()
        {
            var person = new Person();

            Session
                .Store(person);

            Session
                .SaveChanges();

            Session
                .Advanced
                .Refresh(person);

            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/stream1")]
        public HttpResponseMessage Stream1()
        {
            var enumerator = Session.Advanced.Stream<dynamic>(Etag.Empty);
            while (enumerator.MoveNext())
            {
            }

            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/stream2")]
        public HttpResponseMessage Stream2()
        {
            var enumerator = Session.Advanced.Stream<dynamic>("people/");
            while (enumerator.MoveNext())
            {
            }

            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/stream3")]
        public HttpResponseMessage Stream3()
        {
            var enumerator = Session.Advanced.Stream(Session.Query<Person, RavenDocumentsByEntityName>());
            while (enumerator.MoveNext())
            {
            }

            return new HttpResponseMessage();
        }

        [Route("api/sync/session/advanced/whatChanged")]
        public HttpResponseMessage WhatChanged()
        {
            Session.Advanced.WhatChanged();
            return new HttpResponseMessage();
        }
    }
}
