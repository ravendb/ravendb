using System.Collections.Generic;
using System.IO;
using System.Linq;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Util;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Linq;
using Raven.Database.Indexing;
using Raven.Tests.Common;
using Raven.Tests.Common.Analyzers;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class NGramSearch : RavenTest
	{
		public class Image
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public ICollection<string> Users { get; set; }
			public ICollection<string> Tags { get; set; }
		}



		[Fact]
		public void Can_search_inner_words()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new FullTextSearchOnTags.Image { Id = "1", Name = "Great Photo buddy" });
					session.Store(new FullTextSearchOnTags.Image { Id = "2", Name = "Nice Photo of the sky" });
					session.SaveChanges();
				}

				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from doc in docs.Images select new { doc.Name }",
					Indexes =
				                                        	{
				                                        		{"Name", FieldIndexing.Analyzed}
				                                        	},
					Analyzers =
				                                        	{
				                                        		{"Name", typeof (NGramAnalyzer).AssemblyQualifiedName}
				                                        	}
				});

				using (var session = store.OpenSession())
				{
					var images = session.Query<FullTextSearchOnTags.Image>("test")
						.Customize(x => x.WaitForNonStaleResults())
						.OrderBy(x => x.Name)
						.Search(x => x.Name, "phot")
						.ToList();
					Assert.NotEmpty(images);
				}
			}
		}

	}
}
