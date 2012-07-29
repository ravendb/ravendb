// -----------------------------------------------------------------------
//  <copyright file="Maverix2.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Client.Listeners;
using Raven.Database.Extensions;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Maverix2 : IDisposable
	{
		public Maverix2()
		{
			IOExtensions.DeleteDirectory("Maverix");
		}

		public static EmbeddableDocumentStore Store()
		{
			var store = new EmbeddableDocumentStore { DataDirectory = "Maverix" };
			store.Initialize();
			store.RegisterListener(new NonStaleQueryListener());

			new TemplateTests_Search().Execute(store);

			return store;
		}

		[Fact]
		public void WithoutRestart()
		{
			TemplateTest template = new TemplateTest { };
			using (var store = Store())
			{
				using (var session = store.OpenSession())
				{
					session.Store(template);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					Assert.Equal(1, session.Query<TemplateTests_Search.ReduceResult, TemplateTests_Search>().Count());
				}
			}

		}

		[Fact]
		public void WithRestart()
		{
			TemplateTest template = new TemplateTest { };
			using (var store = Store())
			{
				using (var session = store.OpenSession())
				{
					session.Store(template);
					session.SaveChanges();
				}


				using (var session = store.OpenSession())
				{
					Assert.Equal(1, session.Query<TemplateTests_Search.ReduceResult, TemplateTests_Search>().Count());
				}
			}

			using (var store = Store())
			{
				using (var session = store.OpenSession())
				{
					Assert.Equal(1, session.Query<TemplateTests_Search.ReduceResult, TemplateTests_Search>().Count());
				}
			}

		}

		public class NonStaleQueryListener : IDocumentQueryListener
		{
			public void BeforeQueryExecuted(IDocumentQueryCustomization customization)
			{
				customization.WaitForNonStaleResults();
			}
		}

		public void Dispose()
		{
			IOExtensions.DeleteDirectory("Maverix");
		}
	}

	[Serializable]
	public class TemplateTest
	{
		public int Id { get; set; }
		public int masterId { get; set; }
		public string category { get; set; }
		public string name { get; set; }
		public string subject { get; set; }
		public string description { get; set; }
		public string text { get; set; }
		public string html { get; set; }

		public string thumbUrl { get; set; }
		public string previewUrl { get; set; }

		public string[] images { get; set; }
		public long size { get; set; }

		public int state { get; set; }
		public DateTime added { get; set; }
		public DateTime modified { get; set; }

		public int type { get; set; }
		public bool isAutomated { get; set; }
		public long stamp { get; set; }
	}

	public class TemplateTests_Search : AbstractIndexCreationTask<TemplateTest, TemplateTests_Search.ReduceResult>
	{
		public class ReduceResult
		{
			public string name { get; set; }
			public string category { get; set; }
			public string type { get; set; }
			public string[] targetIds { get; set; }
			public string state { get; set; }
			public DateTime added { get; set; }
			public string Query { get; set; }
		}

		public TemplateTests_Search()
		{
			Map = templates => from template in templates
								select new
								{
								name = template.name,
								category = template.category,
								type = template.type,
								state = template.state,
								added = template.added,
								Query = new object[]
								{
									template.name,
									template.category,
									template.subject,
									template.description
								}
								};
			Indexes.Add(x => x.Query, FieldIndexing.Analyzed);
		}
	}
}
