// -----------------------------------------------------------------------
//  <copyright file="ScriptedIndexResultsAndDecimals.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Database.Bundles.ScriptedIndexResults;
using Raven.Database.Config;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class ScriptedIndexResultsAndDecimals : RavenTest
	{
		[Fact]
		public void FailingTest()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new ScriptedIndexResults()
					{
						Id = ScriptedIndexResults.IdPrefix + new Foos().IndexName,
						IndexScript = @"
    var bar = LoadDocument(this.BarId);    
    bar.Wibble = 'hello';
    PutDocument(this.BarId, bar);
",
					});
					session.SaveChanges();
				}

				new Foos().Execute(store);

				string id;
				using (var session = store.OpenSession())
				{
					var foo = new Foo() { Id = "F1", BarId = "B1", Wibble = "hello" };
					session.Store(foo);

					var bar = new Bar() { Id = "B1", D = 1.0m };
					//var bar = new Bar() {Id = "B1", D = 1.5m}; //with this line, the test passes
					session.Store(bar);

					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					var bar = session.Load<Bar>("B1");
					var etagBeforeSave = session.Advanced.GetEtagFor(bar);

					session.SaveChanges();

					var etagAfterSave = session.Advanced.GetEtagFor(bar);

					//at this point the session saves a new version of B1 as the decimal was converted to an integer by the ScriptedIndexResult
					Assert.Equal(etagBeforeSave, (etagAfterSave));
					Assert.Equal(session.Advanced.NumberOfRequests, (1));
				}
			}
		}

		protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
		{
			configuration.Settings["Raven/ActiveBundles"] = "ScriptedIndexResults";
			configuration.Catalog.Catalogs.Add(new TypeCatalog(typeof(ScriptedIndexResultsIndexTrigger)));
		}

		public class Foo
		{
			public string Id { get; set; }
			public string Wibble { get; set; }
			public string BarId { get; set; }
		}

		public class Bar
		{
			public string Id { get; set; }
			public decimal D { get; set; }
			public string Wibble { get; set; }
		}

		public class Foos : AbstractIndexCreationTask<Foo>
		{
			public Foos()
			{
				Map = foos => from foo in foos
							  select new { foo.BarId, foo.Wibble };
			}
		} 
	}
}