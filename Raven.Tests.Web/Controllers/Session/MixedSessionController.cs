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
using System.Threading.Tasks;
using System.Web.Http;

using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Indexes;
using Raven.Tests.Common.Dto;
using Raven.Tests.Web.Models.Transformers;

namespace Raven.Tests.Web.Controllers.Session
{
    public class MixedSessionController : RavenSyncApiController
    {
        [Route("api/mixed/session/load1")]
        public Task<HttpResponseMessage> Load1()
        {
            Session.Load<Person>("people/1");
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/load2")]
        public Task<HttpResponseMessage> Load2()
        {
            Session.Load<Person>("people/1", typeof(PersonTransformer));
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/load3")]
        public Task<HttpResponseMessage> Load3()
        {
            Session.Load<Person>(new[] { "people/1", "people/2" });
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/load4")]
        public Task<HttpResponseMessage> Load4()
        {
            Session.Load<Person>(new[] { "people/1", "people/2" });
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/load5")]
        public Task<HttpResponseMessage> Load5()
        {
            Session.Load<Person>(new[] { "people/1", "people/2" }, typeof(PersonTransformer));
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/load6")]
        public Task<HttpResponseMessage> Load6()
        {
            Session.Load<Person>(1);
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/loadWithInclude1")]
        public Task<HttpResponseMessage> LoadWithInclude1()
        {
            Session
                .Include("AddressId")
                .Load<Person>("people/1");
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/loadWithInclude2")]
        public Task<HttpResponseMessage> LoadWithInclude2()
        {
            Session
                .Include("AddressId")
                .Load<Person>(new[] { "people/1", "people/2" });
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/query")]
        public Task<HttpResponseMessage> Query()
        {
            Session
                .Query<Person>()
                .ToList();
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/saveChanges")]
        public Task<HttpResponseMessage> SaveChanges()
        {
            Session
                .SaveChanges();
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/store")]
        public Task<HttpResponseMessage> Store()
        {
            Session
                .Store(new Person());
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/delete1")]
        public Task<HttpResponseMessage> Delete1()
        {
            Session
                .Delete("people/1");
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/delete2")]
        public Task<HttpResponseMessage> Delete2()
        {
            Session
                .Delete<Person>(1);
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/clear")]
        public Task<HttpResponseMessage> Clear()
        {
            Session
                .Advanced
                .Clear();
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/defer")]
        public Task<HttpResponseMessage> Defer()
        {
            Session
                .Advanced
                .Defer(new DeleteCommandData { Key = "keys/1" });
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/documentQuery")]
        public Task<HttpResponseMessage> DocumentQuery()
        {
            Session
                .Advanced
                .DocumentQuery<Person>()
                .ToList();
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/eagerly/executeAllPendingLazyOperations")]
        public Task<HttpResponseMessage> ExecuteAllPendingLazyOperations()
        {
            Session
                .Advanced
                .Defer(new DeleteCommandData { Key = "keys/1" });

            Session
                .Advanced
                .Eagerly
                .ExecuteAllPendingLazyOperations();
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/evict")]
        public Task<HttpResponseMessage> Evict()
        {
            var person = new Person();

            Session
                .Store(person);

            Session
                .Advanced
                .Evict(person);
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/explicitlyVersion")]
        public Task<HttpResponseMessage> ExplicitlyVersion()
        {
            var person = new Person();

            Session
                .Store(person);

            Session
                .Advanced
                .ExplicitlyVersion(person);
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/getDocumentId")]
        public Task<HttpResponseMessage> GetDocumentId()
        {
            var person = new Person();

            Session
                .Store(person);

            Session
                .Advanced
                .GetDocumentId(person);
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/getDocumentUrl")]
        public Task<HttpResponseMessage> GetDocumentUrl()
        {
            var person = new Person();

            Session
                .Store(person);

            Session
                .Advanced
                .GetDocumentUrl(person);
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/getEtagFor")]
        public Task<HttpResponseMessage> GetEtagFor()
        {
            var person = new Person();

            Session
                .Store(person);

            Session
                .Advanced
                .GetEtagFor(person);
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/getMetadataFor")]
        public Task<HttpResponseMessage> GetMetadataFor()
        {
            var person = new Person();

            Session
                .Store(person);

            Session
                .Advanced
                .GetMetadataFor(person);
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/hasChanged")]
        public Task<HttpResponseMessage> HasChanged()
        {
            var person = new Person();

            Session
                .Store(person);

            Session
                .Advanced
                .HasChanged(person);
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/ignoreChangesFor")]
        public Task<HttpResponseMessage> IgnoreChangesFor()
        {
            var person = new Person();

            Session
                .Store(person);

            Session
                .Advanced
                .IgnoreChangesFor(person);
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/isLoaded")]
        public Task<HttpResponseMessage> IsLoaded()
        {
            var person = new Person();

            Session
                .Store(person);

            Session
                .Advanced
                .IsLoaded("people/1");
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/lazily/loadWithInclude1")]
        public Task<HttpResponseMessage> LazyLoadWithInclude1()
        {
            var lazy = Session
                .Advanced
                .Lazily
                .Include("AddressId")
                .Load<Person>("people/1");

            var person = lazy.Value;
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/lazily/loadWithInclude2")]
        public Task<HttpResponseMessage> LazyLoadWithInclude2()
        {
            var lazy = Session
                .Advanced
                .Lazily
                .Include("AddressId")
                .Load<Person>(new[] { "people/1", "people/2" });

            var people = lazy.Value;
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/lazily/load1")]
        public Task<HttpResponseMessage> LazyLoad1()
        {
            var lazy = Session
                .Advanced
                .Lazily
                .Load<Person>("people/1");

            var person = lazy.Value;
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/lazily/load2")]
        public Task<HttpResponseMessage> LazyLoad2()
        {
            var lazy = Session
                .Advanced
                .Lazily
                .Load<Person>(new[] { "people/1", "people/2" });

            var people = lazy.Value;
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/lazily/loadStartingWith")]
        public Task<HttpResponseMessage> LazyLoadStartingWith()
        {
            var lazy = Session
                .Advanced
                .Lazily
                .LoadStartingWith<Person>("people/1");

            var people = lazy.Value;
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/lazily/moreLikeThis")]
        public Task<HttpResponseMessage> LazyMoreLikeThis()
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
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/loadStartingWith")]
        public Task<HttpResponseMessage> LoadStartingWith()
        {
            Session
                .Advanced
                .LoadStartingWith<Person>("people/");

            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/luceneQuery")]
        public Task<HttpResponseMessage> LuceneQuery()
        {
            Session
                .Advanced
                .LuceneQuery<Person>()
                .ToList();

            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/markReadOnly")]
        public Task<HttpResponseMessage> MarkReadOnly()
        {
            var person = new Person();

            Session
                .Store(person);

            Session
                .Advanced
                .MarkReadOnly(person);

            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/multiFacetedSearch")]
        public Task<HttpResponseMessage> MultiFacetedSearch()
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

            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/refresh")]
        public Task<HttpResponseMessage> Refresh()
        {
            var person = new Person();

            Session
                .Store(person);

            Session
                .SaveChanges();

            Session
                .Advanced
                .Refresh(person);

            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/stream1")]
        public Task<HttpResponseMessage> Stream1()
        {
            var enumerator = Session.Advanced.Stream<dynamic>(Etag.Empty);
            while (enumerator.MoveNext())
            {
            }

            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/stream2")]
        public Task<HttpResponseMessage> Stream2()
        {
            var enumerator = Session.Advanced.Stream<dynamic>("people/");
            while (enumerator.MoveNext())
            {
            }

            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/stream3")]
        public Task<HttpResponseMessage> Stream3()
        {
            var enumerator = Session.Advanced.Stream(Session.Query<Person, RavenDocumentsByEntityName>());
            while (enumerator.MoveNext())
            {
            }

            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }

        [Route("api/mixed/session/advanced/whatChanged")]
        public Task<HttpResponseMessage> WhatChanged()
        {
            Session.Advanced.WhatChanged();
            return new CompletedTask<HttpResponseMessage>(new HttpResponseMessage());
        }
    }
}
