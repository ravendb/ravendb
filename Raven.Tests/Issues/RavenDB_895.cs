// -----------------------------------------------------------------------
//  <copyright file="RavenDB_895.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
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
		public void TransformScriptFiltering()
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
				using (var documentStore = NewRemoteDocumentStore())
				{
					using (var session = documentStore.OpenSession())
					{
						session.Store(new Foo { Name = "N1" });
						session.Store(new Foo { Name = "N2" });

						session.SaveChanges();
					}
					var smugglerApi = new SmugglerApi(options, new RavenConnectionStringOptions
					{
						Url = documentStore.Url
					});
					smugglerApi.ExportData(null, options, false).Wait(TimeSpan.FromSeconds(15));
				}

				using (var documentStore = NewRemoteDocumentStore())
				{
					var smugglerApi = new SmugglerApi(options, new RavenConnectionStringOptions
					{
						Url = documentStore.Url
					});
					smugglerApi.ImportData(options).Wait(TimeSpan.FromSeconds(15));

					using (var session = documentStore.OpenSession())
					{
						var foos = session
							.Query<Foo>()
							.ToList();

						Assert.Equal(1, foos.Count);
						Assert.Equal("foos/2", foos[0].Id);
						Assert.Equal("N2", foos[0].Name);
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
		public void TransformScriptModifying()
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
				using (var documentStore = NewRemoteDocumentStore())
				{
					using (var session = documentStore.OpenSession())
					{
						session.Store(new Foo { Name = "N1" });
						session.Store(new Foo { Name = "N2" });

						session.SaveChanges();
					}
					var smugglerApi = new SmugglerApi(options, new RavenConnectionStringOptions
					{
						Url = documentStore.Url
					});
					smugglerApi.ExportData(null, options, false).Wait(TimeSpan.FromSeconds(15));
				}

				using (var documentStore = NewRemoteDocumentStore())
				{
					var smugglerApi = new SmugglerApi(options, new RavenConnectionStringOptions
					{
						Url = documentStore.Url
					});
					smugglerApi.ImportData(options).Wait(TimeSpan.FromSeconds(15));

					using (var session = documentStore.OpenSession())
					{
						var foos = session
							.Query<Foo>()
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