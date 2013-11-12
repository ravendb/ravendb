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
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_895 : RavenTest
	{
		private class Foo
		{
			public string Id { get; set; }

			public string Name { get; set; }
		}

		[Fact]
		public async Task TransformScriptFiltering()
		{
			var options = new SmugglerOptions
			{
				BackupPath = Path.GetTempFileName(),
				TransformScript = @"function(doc) { 
						var id = doc['@metadata']['@id']; 
						if(id === 'foos/1')
							return null;
						return doc;
					}"
			};

			try
			{
				using (var store = NewRemoteDocumentStore())
				{
					using (var session = store.OpenSession())
					{
						session.Store(new Foo {Name = "N1"});
						session.Store(new Foo {Name = "N2"});

						session.SaveChanges();
					}
					var smugglerApi = new SmugglerApi(new RavenConnectionStringOptions
					{
						Url = store.Url
					});
					await smugglerApi.ExportData(options);
				}

				using (var documentStore = NewRemoteDocumentStore())
				{
					var smugglerApi = new SmugglerApi(new RavenConnectionStringOptions
					{
						Url = documentStore.Url
					});
					await smugglerApi.ImportData(options);

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
			finally
			{
				if (File.Exists(options.BackupPath))
				{
					File.Delete(options.BackupPath);
				}
			}
		}

		[Fact]
		public async Task TransformScriptModifying()
		{
			var options = new SmugglerOptions
			{
				BackupPath = Path.GetTempFileName(),
				TransformScript = @"function(doc) { 
						doc['Name'] = 'Changed';
						return doc;
					}"
			};

			try
			{
				using (var store = NewRemoteDocumentStore())
				{
					using (var session = store.OpenSession())
					{
						session.Store(new Foo { Name = "N1" });
						session.Store(new Foo { Name = "N2" });

						session.SaveChanges();
					}
					var smugglerApi = new SmugglerApi(new RavenConnectionStringOptions
					{
						Url = store.Url
					});
					await smugglerApi.ExportData(options);
				}

				using (var store = NewRemoteDocumentStore())
				{
					var smugglerApi = new SmugglerApi(new RavenConnectionStringOptions
					{
						Url = store.Url
					});
					await smugglerApi.ImportData(options);

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
			finally
			{
				if (File.Exists(options.BackupPath))
				{
					File.Delete(options.BackupPath);
				}
			}
		}
	}
}