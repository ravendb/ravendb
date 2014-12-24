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
		public async Task TransformScriptFiltering()
		{
	        using (var store = NewRemoteDocumentStore())
	        {
	            using (var session = store.OpenSession())
	            {
	                session.Store(new Foo {Name = "N1"});
	                session.Store(new Foo {Name = "N2"});

	                session.SaveChanges();
	            }
                var smugglerApi = new SmugglerDatabaseApi();
		        smugglerApi.Options.TransformScript =
			        @"function(doc) { 
						var id = doc['@metadata']['@id']; 
						if(id === 'foos/1')
							return null;
						return doc;
					}";
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

	        using (var documentStore = NewRemoteDocumentStore())
	        {
                var smugglerApi = new SmugglerDatabaseApi();
		        smugglerApi.Options.TransformScript =
			        @"function(doc) { 
						var id = doc['@metadata']['@id']; 
						if(id === 'foos/1')
							return null;
						return doc;
					}";
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
		public async Task TransformScriptModifying()
		{
	        using (var store = NewRemoteDocumentStore())
	        {
	            using (var session = store.OpenSession())
	            {
	                session.Store(new Foo { Name = "N1" });
	                session.Store(new Foo { Name = "N2" });

	                session.SaveChanges();
	            }
                var smugglerApi = new SmugglerDatabaseApi();
		        smugglerApi.Options.TransformScript =
			        @"function(doc) { 
						doc['Name'] = 'Changed';
						return doc;
					}";
		        await smugglerApi.ExportData(
                    new SmugglerExportOptions<RavenConnectionStringOptions>
			        {
				        From = new RavenConnectionStringOptions
				        {
					        Url = store.Url,
					        DefaultDatabase = store.DefaultDatabase
				        },
				        ToFile = file,
			        });
	        }

	        using (var store = NewRemoteDocumentStore())
	        {
                var smugglerApi = new SmugglerDatabaseApi();
		        smugglerApi.Options.TransformScript =
			        @"function(doc) { 
						doc['Name'] = 'Changed';
						return doc;
					}";
		        await smugglerApi.ImportData(
                    new SmugglerImportOptions<RavenConnectionStringOptions>
			        {
				        To = new RavenConnectionStringOptions
				        {
					        Url = store.Url,
					        DefaultDatabase = store.DefaultDatabase
				        },
				        FromFile = file,
			        });

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
    }
}
