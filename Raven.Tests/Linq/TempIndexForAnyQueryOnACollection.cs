// -----------------------------------------------------------------------
//  <copyright file="Class1.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Embedded;
using Xunit;

namespace Raven.Tests.Linq
{
	public class TempIndexForAnyQueryOnACollection : RavenTest
	{
		private readonly EmbeddableDocumentStore store;

		public TempIndexForAnyQueryOnACollection()
		{
			store = NewDocumentStore();
		}

		[Fact]
		public void Test1()
		{
			using (var session = store.OpenSession())
			{
				session.Store(new ResourceEntry
				{
					Key = "hello-world",
					Text = "hello world",
					Translations = new[]
					{
						new ResourceEntry.Translation {CultureCode = "es-ES", Text = "hola mundo"},
						new ResourceEntry.Translation {CultureCode = "de-DE", Text = "hallo welt"},
						new ResourceEntry.Translation {CultureCode = "fr-FR", Text = "bonjour tout le monde"}
					}
				});

				session.SaveChanges();
			}

			using (var session = store.OpenSession())
			{
				var nonTranslatedEntries =
					session.Query<ResourceEntry>()
					       .Customize(x => x.WaitForNonStaleResults())
					       .Where(e => e.Translations == null ||
					                   !e.Translations.Any(t => t.CultureCode == "es-ES" && t.Text != null))
					       .ToList();

				WaitForUserToContinueTheTest(store);

				Assert.Equal(0, nonTranslatedEntries.Count);
			}
		}

		[Fact]
		public void Test2()
		{
			using (var session = store.OpenSession())
			{
				session.Store(new ResourceEntry
				{
					Key = "hello-world",
					Text = "hello world",
					Translations = new[]
					{
						new ResourceEntry.Translation {CultureCode = "es-ES", Text = "hola mundo"}
					}
				});

				session.SaveChanges();
			}

			using (var session = store.OpenSession())
			{
				var nonTranslatedEntries =
					session.Query<ResourceEntry>()
					       .Customize(x => x.WaitForNonStaleResults())
					       .Where(e => e.Translations == null ||
					                   !e.Translations.Any(t => t.CultureCode == "es-ES" && t.Text != null))
					       .ToList();

				Assert.Equal(0, nonTranslatedEntries.Count);
			}
		}

		[Fact]
		public void Test3()
		{
			using (var session = store.OpenSession())
			{
				session.Store(new ResourceEntry
				{
					Key = "hello-world",
					Text = "hello world",
					Translations = new[]
					{
						new ResourceEntry.Translation {CultureCode = "es-ES", Text = null},
						new ResourceEntry.Translation {CultureCode = "de-DE", Text = "hallo welt"},
						new ResourceEntry.Translation {CultureCode = "fr-FR", Text = "bonjour tout le monde"}
					}
				});

				session.SaveChanges();
			}

			using (var session = store.OpenSession())
			{
				var nonTranslatedEntries =
					session.Query<ResourceEntry>()
					       .Customize(x => x.WaitForNonStaleResults())
					       .Where(e => e.Translations == null ||
					                   !e.Translations.Any(t => t.CultureCode == "es-ES" && t.Text != null))
					       .ToList();

				Assert.Equal(1, nonTranslatedEntries.Count);
			}
		}

		[Fact]
		public void Test4()
		{
			using (var session = store.OpenSession())
			{
				session.Store(new ResourceEntry
				{
					Key = "hello-world",
					Text = "hello world",
					Translations = new[]
					{
						new ResourceEntry.Translation {CultureCode = "de-DE", Text = "hallo welt"},
						new ResourceEntry.Translation {CultureCode = "fr-FR", Text = "bonjour tout le monde"}
					}
				});

				session.SaveChanges();
			}

			using (var session = store.OpenSession())
			{
				var nonTranslatedEntries =
					session.Query<ResourceEntry>()
					       .Customize(x => x.WaitForNonStaleResults())
					       .Where(e => e.Translations == null ||
					                   !e.Translations.Any(t => t.CultureCode == "es-ES" && t.Text != null))
					       .ToList();

				Assert.Equal(1, nonTranslatedEntries.Count);
			}
		}

		private class ResourceEntry
		{
			public string Key { get; set; }
			public string Text { get; set; }
			public IList<Translation> Translations { get; set; }

			public class Translation
			{
				public string CultureCode { get; set; }
				public string Text { get; set; }
			}
		}
	}
}