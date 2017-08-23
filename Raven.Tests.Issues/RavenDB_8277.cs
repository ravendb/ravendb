using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Smuggler;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_8277 : RavenTestBase
    {
        [Fact, Trait("Category", "Smuggler")]
        public async Task Filtering_between_with_subscriptions_should_work()
        {
            using (var serverFrom = GetNewServer(8090))
            using (var serverTo = GetNewServer(8091))
            using (var from = NewRemoteDocumentStore(ravenDbServer: serverFrom))
            using (var to = NewRemoteDocumentStore(ravenDbServer: serverTo))
            {
                using (var session = from.OpenSession())
                {
                    session.Store(new { Foo = "Bar" }, "foo/bar");
                    session.SaveChanges();
                }

                from.Subscriptions.Create(new SubscriptionCriteria
                {
                    KeyStartsWith = "foo/b"
                });

                var filters = new FilterSetting
                {
                    Values = new[] { "Raven/Subscriptions" }.ToList(),
                    ShouldMatch = false,
                    Path = "@name"
                };

                var smuggler = new SmugglerDatabaseApi(
                    new SmugglerDatabaseOptions
                    {
                        Incremental = false,
                        Filters = new[] { filters }.ToList(),
                        TransformScript = null,
                    });

                await smuggler.Between(new SmugglerBetweenOptions<RavenConnectionStringOptions>
                {
                    From = new RavenConnectionStringOptions
                    {
                        Url = from.Url,
                        DefaultDatabase = from.DefaultDatabase
                    },
                    To = new RavenConnectionStringOptions
                    {
                        Url = to.Url,
                        DefaultDatabase = to.DefaultDatabase
                    }
                });
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task Filtering_with_export_and_import_subscriptions_should_work()
        {
            using (var store = NewRemoteDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new { Foo = "Bar" });
                    session.SaveChanges();
                }

                store.Subscriptions.Create(new SubscriptionCriteria
                {
                    KeyStartsWith = "foo/b"
                });

                var filters = new FilterSetting
                {
                    Values = new[] { "Raven/Subscriptions" }.ToList(),
                    ShouldMatch = false,
                    Path = "@name"
                };


                var smuggler = new SmugglerDatabaseApi(
                    new SmugglerDatabaseOptions
                    {
                        Incremental = false,
                        Filters = new[] { filters }.ToList(),
                        TransformScript = null,
                    });

                using (var dummyStream = new MemoryStream())
                {
                    //this shouldn't throw exception...
                    await smuggler.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions>
                    {
                        From = new RavenConnectionStringOptions
                        {
                            Url = store.Url,
                            DefaultDatabase = store.DefaultDatabase
                        },
                        ToStream = dummyStream
                    });

                    dummyStream.Position = 0;

                    await smuggler.ImportData(new SmugglerImportOptions<RavenConnectionStringOptions>
                    {
                        FromStream = dummyStream,
                        To = new RavenConnectionStringOptions
                        {
                            Url = store.Url,
                            DefaultDatabase = store.DefaultDatabase
                        }
                    });
                }
            }
        }
    }
}
