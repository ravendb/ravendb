using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Indexes
{
	public class HighlightTesting : RavenTestBase
	{
		[Fact]
		public void HighlightText()
		{
			var item = new SearchItem
			{
				Id = "searchitems/1",
				Name = "This is a sample about a dog"
			};

			var searchFor = "dog";
			using(var store = NewDocumentStore().Initialize())
			using (var session = store.OpenSession())
			{
				session.Store(item);
				store.DatabaseCommands.PutIndex(new ContentSearchIndex().IndexName, new ContentSearchIndex().CreateIndexDefinition());
				session.SaveChanges();
				FieldHighlightings nameHighlighting;
				var results = session.Advanced.LuceneQuery<SearchItem>("ContentSearchIndex")
				                     .WaitForNonStaleResults()
				                     .Highlight("Name", 512, 1, out nameHighlighting)
									 .Search("Name", searchFor)
				                     .ToArray();
				Assert.NotEmpty(nameHighlighting.GetFragments("searchitems/1"));
				Assert.Equal("This is a sample about a <b style=\"background:yellow\">dog</b>",
				             nameHighlighting.GetFragments("searchitems/1").First());
			}
		}
	}

	public class ContentSearchIndex : AbstractIndexCreationTask<SearchItem>
	{
		public ContentSearchIndex()
		{
			Map = (docs => from doc in docs
			                           select new {doc.Name});

			Index(x => x.Name, FieldIndexing.Analyzed);
			Store(x => x.Name, FieldStorage.Yes);
			TermVector(x => x.Name, FieldTermVector.WithPositionsAndOffsets);
		}
	}

	public class SearchItem
	{
		public string Name { get; set; }
		public string Id { get; set; }
	}
}
