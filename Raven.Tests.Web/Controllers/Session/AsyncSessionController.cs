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
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Common.Dto;
using Raven.Tests.Web.Models.Transformers;

namespace Raven.Tests.Web.Controllers.Session
{
    public class AsyncSessionController : RavenAsyncApiController
    {
        [Route("api/async/session/load1")]
        public async Task<HttpResponseMessage> Load1()
        {
            await Session.LoadAsync<Person>("people/1");
            return new HttpResponseMessage();
        }

        [Route("api/async/session/load2")]
        public async Task<HttpResponseMessage> Load2()
        {
            await Session.LoadAsync<Person>("people/1", typeof(PersonTransformer));
            return new HttpResponseMessage();
        }

        [Route("api/async/session/load3")]
        public async Task<HttpResponseMessage> Load3()
        {
            await Session.LoadAsync<Person>(new[] { "people/1", "people/2" });
            return new HttpResponseMessage();
        }

        [Route("api/async/session/load4")]
        public async Task<HttpResponseMessage> Load4()
        {
            await Session.LoadAsync<Person>(new[] { "people/1", "people/2" });
            return new HttpResponseMessage();
        }

        [Route("api/async/session/load5")]
        public async Task<HttpResponseMessage> Load5()
        {
            await Session.LoadAsync<Person>(new[] { "people/1", "people/2" }, typeof(PersonTransformer));
            return new HttpResponseMessage();
        }

        [Route("api/async/session/load6")]
        public async Task<HttpResponseMessage> Load6()
        {
            await Session.LoadAsync<Person>(1);
            return new HttpResponseMessage();
        }

        [Route("api/async/session/loadWithInclude1")]
        public async Task<HttpResponseMessage> LoadWithInclude1()
        {
            await Session
                .Include("AddressId")
                .LoadAsync<Person>("people/1");
            return new HttpResponseMessage();
        }

        [Route("api/async/session/loadWithInclude2")]
        public async Task<HttpResponseMessage> LoadWithInclude2()
        {
            await Session
                .Include("AddressId")
                .LoadAsync<Person>(new[] { "people/1", "people/2" });
            return new HttpResponseMessage();
        }

        [Route("api/async/session/query")]
        public async Task<HttpResponseMessage> Query()
        {
            await Session
                .Query<Person>()
                .ToListAsync();
            return new HttpResponseMessage();
        }

        [Route("api/async/session/saveChanges")]
        public async Task<HttpResponseMessage> SaveChanges()
        {
            await Session
                .SaveChangesAsync();
            return new HttpResponseMessage();
        }

        [Route("api/async/session/store")]
        public async Task<HttpResponseMessage> Store()
        {
            await Session
                .StoreAsync(new Person());
            return new HttpResponseMessage();
        }

        [Route("api/async/session/delete1")]
        public async Task<HttpResponseMessage> Delete1()
        {
            Session
                .Delete("people/1");
            return new HttpResponseMessage();
        }

        [Route("api/async/session/delete2")]
        public async Task<HttpResponseMessage> Delete2()
        {
            Session
                .Delete<Person>(1);
            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/clear")]
        public async Task<HttpResponseMessage> Clear()
        {
            Session
                .Advanced
                .Clear();
            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/defer")]
        public async Task<HttpResponseMessage> Defer()
        {
            Session
                .Advanced
                .Defer(new DeleteCommandData { Key = "keys/1" });
            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/documentQuery")]
        public async Task<HttpResponseMessage> DocumentQuery()
        {
            await Session
                .Advanced
                .AsyncDocumentQuery<Person>()
                .ToListAsync();
            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/eagerly/executeAllPendingLazyOperations")]
        public async Task<HttpResponseMessage> ExecuteAllPendingLazyOperations()
        {
            Session
                .Advanced
                .Defer(new DeleteCommandData { Key = "keys/1" });

            await Session
                .Advanced
                .Eagerly
                .ExecuteAllPendingLazyOperationsAsync();
            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/evict")]
        public async Task<HttpResponseMessage> Evict()
        {
            var person = new Person();

            await Session
                .StoreAsync(person);

            Session
                .Advanced
                .Evict(person);
            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/explicitlyVersion")]
        public async Task<HttpResponseMessage> ExplicitlyVersion()
        {
            var person = new Person();

            await Session
                .StoreAsync(person);

            Session
                .Advanced
                .ExplicitlyVersion(person);
            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/getDocumentId")]
        public async Task<HttpResponseMessage> GetDocumentId()
        {
            var person = new Person();

            await Session
                .StoreAsync(person);

            Session
                .Advanced
                .GetDocumentId(person);
            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/getDocumentUrl")]
        public async Task<HttpResponseMessage> GetDocumentUrl()
        {
            var person = new Person();

            await Session
                .StoreAsync(person);

            Session
                .Advanced
                .GetDocumentUrl(person);
            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/getEtagFor")]
        public async Task<HttpResponseMessage> GetEtagFor()
        {
            var person = new Person();

            await Session
                .StoreAsync(person);

            Session
                .Advanced
                .GetEtagFor(person);
            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/getMetadataFor")]
        public async Task<HttpResponseMessage> GetMetadataFor()
        {
            var person = new Person();

            await Session
                .StoreAsync(person);

            Session
                .Advanced
                .GetMetadataFor(person);
            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/hasChanged")]
        public async Task<HttpResponseMessage> HasChanged()
        {
            var person = new Person();

            await Session
                .StoreAsync(person);

            Session
                .Advanced
                .HasChanged(person);
            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/ignoreChangesFor")]
        public async Task<HttpResponseMessage> IgnoreChangesFor()
        {
            var person = new Person();

            await Session
                .StoreAsync(person);

            Session
                .Advanced
                .IgnoreChangesFor(person);
            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/isLoaded")]
        public async Task<HttpResponseMessage> IsLoaded()
        {
            var person = new Person();

            await Session
                .StoreAsync(person);

            Session
                .Advanced
                .IsLoaded("people/1");
            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/lazily/loadWithInclude1")]
        public async Task<HttpResponseMessage> LazyLoadWithInclude1()
        {
            var lazy = Session
                .Advanced
                .Lazily
                .Include("AddressId")
                .LoadAsync<Person>("people/1");

            var person = await lazy.Value;
            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/lazily/loadWithInclude2")]
        public async Task<HttpResponseMessage> LazyLoadWithInclude2()
        {
            var lazy = Session
                .Advanced
                .Lazily
                .Include("AddressId")
                .LoadAsync<Person>(new[] { "people/1", "people/2" });

            var people = await lazy.Value;
            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/lazily/load1")]
        public async Task<HttpResponseMessage> LazyLoad1()
        {
            var lazy = Session
                .Advanced
                .Lazily
                .LoadAsync<Person>("people/1");

            var person = await lazy.Value;
            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/lazily/load2")]
        public async Task<HttpResponseMessage> LazyLoad2()
        {
            var lazy = Session
                .Advanced
                .Lazily
                .LoadAsync<Person>(new[] { "people/1", "people/2" });

            var people = await lazy.Value;
            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/lazily/loadStartingWith")]
        public async Task<HttpResponseMessage> LazyLoadStartingWith()
        {
            var lazy = Session
                .Advanced
                .Lazily
                .LoadStartingWithAsync<Person>("people/1");

            var people = await lazy.Value;
            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/lazily/moreLikeThis")]
        public async Task<HttpResponseMessage> LazyMoreLikeThis()
        {
            var key = Guid.NewGuid().ToString();
            await Session.StoreAsync(new Person { Id = key });
            await Session.SaveChangesAsync();

            SpinWait.SpinUntil(() => DocumentStore.DatabaseCommands.GetStatistics().StaleIndexes.Length == 0);

            var lazy = Session
                .Advanced
                .Lazily
                .MoreLikeThisAsync<dynamic>(new MoreLikeThisQuery
                                      {
                                          IndexName = new RavenDocumentsByEntityName().IndexName,
                                          DocumentId = key
                                      });

            var people = await lazy.Value;
            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/loadStartingWith")]
        public async Task<HttpResponseMessage> LoadStartingWith()
        {
            await Session
                .Advanced
                .LoadStartingWithAsync<Person>("people/");

            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/luceneQuery")]
        public async Task<HttpResponseMessage> LuceneQuery()
        {
            await Session
                .Advanced
                .AsyncLuceneQuery<Person>()
                .ToListAsync();

            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/markReadOnly")]
        public async Task<HttpResponseMessage> MarkReadOnly()
        {
            var person = new Person();

            await Session
                .StoreAsync(person);

            Session
                .Advanced
                .MarkReadOnly(person);

            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/multiFacetedSearch")]
        public async Task<HttpResponseMessage> MultiFacetedSearch()
        {
            var person = new Person();

            await Session
                .StoreAsync(person);

            await Session
                .Advanced
                .MultiFacetedSearchAsync(new[]
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

        [Route("api/async/session/advanced/refresh")]
        public async Task<HttpResponseMessage> Refresh()
        {
            var person = new Person();

            await Session
                .StoreAsync(person);

            await Session
                .SaveChangesAsync();

            await Session
                .Advanced
                .RefreshAsync(person);

            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/stream1")]
        public async Task<HttpResponseMessage> Stream1()
        {
            var enumerator = await Session.Advanced.StreamAsync<dynamic>(Etag.Empty);
            while (await enumerator.MoveNextAsync())
            {
            }

            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/stream2")]
        public async Task<HttpResponseMessage> Stream2()
        {
            var enumerator = await Session.Advanced.StreamAsync<dynamic>("people/");
            while (await enumerator.MoveNextAsync())
            {
            }

            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/stream3")]
        public async Task<HttpResponseMessage> Stream3()
        {
            var enumerator = await Session.Advanced.StreamAsync(Session.Query<Person, RavenDocumentsByEntityName>());
            while (await enumerator.MoveNextAsync())
            {
            }

            return new HttpResponseMessage();
        }

        [Route("api/async/session/advanced/whatChanged")]
        public async Task<HttpResponseMessage> WhatChanged()
        {
            Session.Advanced.WhatChanged();
            return new HttpResponseMessage();
        }
    }
}
