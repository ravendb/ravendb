// -----------------------------------------------------------------------
//  <copyright file="RavenDB_895.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Client.Document;
using Raven.Smuggler;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Smuggler
{
    public class SmugglerTransformScriptsTests : RavenTest
    {
        private readonly string file;

        private class Foo
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public string NameImport { get; set; }
        }

        public SmugglerTransformScriptsTests()
        {
            file = Path.GetTempFileName();
        }

        public override void Dispose()
        {
            base.Dispose();
            if (File.Exists(file))
                File.Delete(file);
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task TransformScriptFiltering_Export()
        {
            using (var store = NewRemoteDocumentStore())
            {
                StoreTwoFooItems(store);

                await Export(store, @"function(doc) { 
                        var id = doc['@metadata']['@id']; 
                        if(id === 'foos/1')
                            return null;
                        return doc;
                    }");
            }

            using (var documentStore = NewRemoteDocumentStore())
            {
                await Import(documentStore, string.Empty);

                using (var session = documentStore.OpenSession())
                {
                    var foos = session.Query<Foo>()
                                      .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                                      .ToList();

                    Assert.Equal(1, foos.Count);
                    Assert.Equal("foos/2", foos[0].Id);
                    Assert.Equal("N2", foos[0].Name);

                    Assert.Null(session.Load<Foo>(1));
                }
            }
        }

        [Fact]
        public async Task TransformScriptFiltering_Import()
        {
            using (var store = NewRemoteDocumentStore())
            {
                StoreTwoFooItems(store);

                await Export(store, string.Empty);
            }

            using (var documentStore = NewRemoteDocumentStore())
            {
                await Import(documentStore, @"function(doc) { 
                        var id = doc['@metadata']['@id']; 
                        if(id === 'foos/1')
                            return null;
                        return doc;
                    }");

                using (var session = documentStore.OpenSession())
                {
                    var foos = session.Query<Foo>()
                                      .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                                      .ToList();

                    Assert.Equal(1, foos.Count);
                    Assert.Equal("foos/2", foos[0].Id);
                    Assert.Equal("N2", foos[0].Name);

                    Assert.Null(session.Load<Foo>(1));
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task TransformScriptModifying_Import()
        {
            using (var store = NewRemoteDocumentStore())
            {
                StoreTwoFooItems(store);

                await Export(store, string.Empty);
            }

            using (var store = NewRemoteDocumentStore())
            {
                await Import(store, @"function(doc) { 
                        doc['Name'] = 'Changed';
                        return doc;
                    }");

                using (var session = store.OpenSession())
                {
                    var foos = session.Query<Foo>()
                                      .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                                      .ToList();

                    Assert.Equal(2, foos.Count);

                    foreach (var foo in foos)
                    {
                        Assert.Equal("Changed", foo.Name);
                    }
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task TransformScriptModifying_Export()
        {
            using (var store = NewRemoteDocumentStore())
            {
                StoreTwoFooItems(store);

                await Export(store, @"function(doc) { 
                        doc['Name'] = 'Changed';
                        return doc;
                    }");
            }

            using (var store = NewRemoteDocumentStore())
            {
                await Import(store, string.Empty);

                using (var session = store.OpenSession())
                {
                    var foos = session.Query<Foo>()
                                      .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                                      .ToList();

                    Assert.Equal(2, foos.Count);

                    foreach (var foo in foos)
                    {
                        Assert.Equal("Changed", foo.Name);
                    }
                }
            }
        }

        private static void StoreTwoFooItems(DocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Foo { Name = "N1" });
                session.Store(new Foo { Name = "N2" });

                session.SaveChanges();
            }
        }

        private async Task Export(DocumentStore store, string transformScript)
        {
            var smugglerApi = new SmugglerDatabaseApi();
            smugglerApi.Options.TransformScript = transformScript;

            await smugglerApi.ExportData(
                new SmugglerExportOptions<RavenConnectionStringOptions>
                {
                    ToFile = file,
                    From = new RavenConnectionStringOptions
                    {
                        Url = store.Url,
                        DefaultDatabase = store.DefaultDatabase
                    }
                });
        }

        private async Task Import(DocumentStore documentStore, string transformScript)
        {
            var smugglerApi = new SmugglerDatabaseApi();
            smugglerApi.Options.TransformScript = transformScript;
            ;
            await smugglerApi.ImportData(
                new SmugglerImportOptions<RavenConnectionStringOptions>
                {
                    FromFile = file,
                    To = new RavenConnectionStringOptions
                    {
                        Url = documentStore.Url,
                        DefaultDatabase = documentStore.DefaultDatabase
                    }
                });
        }
    }
}
