using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Analysis.Tokenattributes;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.QueryParsers;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Abstractions.Indexing;
using Raven.Database.Config;
using Raven.Database.Indexing;
using Raven.Tests.Indexes;
using Xunit;
using Version = Lucene.Net.Util.Version;

namespace Raven.Tests.Bugs.Indexing
{
	public class WiseShrek : RavenTest
	{

		public class Soft
		{
			public int f_platform;
			public string f_name;
			public string f_alias;
			public string f_License;
			public int f_totaldownload;
		}

		[Fact]
		public void Isolated()
		{
			var ramDirectory = new RAMDirectory();
			using (new IndexWriter(ramDirectory, new StandardAnalyzer(Version.LUCENE_29), IndexWriter.MaxFieldLength.UNLIMITED)){}
			var simpleIndex = new SimpleIndex(ramDirectory, "test", new IndexDefinition
			{
				Map =
			                                                        	@"from s in docs.Softs select new { s.f_platform, s.f_name, s.f_alias,s.f_License,s.f_totaldownload}",
				Analyzers =
			                                                        	{
			                                                        		{"f_name", typeof (KeywordAnalyzer).AssemblyQualifiedName},
			                                                        		{"f_alias", typeof (KeywordAnalyzer).AssemblyQualifiedName},
			                                                        	},
				Indexes =
			                                                        	{
			                                                        		{"f_platform", FieldIndexing.NotAnalyzed},
			                                                        		{"f_License", FieldIndexing.NotAnalyzed},
			                                                        		{"f_totaldownload", FieldIndexing.NotAnalyzed},
			                                                        		{"f_name", FieldIndexing.Analyzed},
			                                                        		{"f_alias", FieldIndexing.Analyzed},
			                                                        	},
				SortOptions =
			                                                        	{
			                                                        		{"f_totaldownload", SortOptions.Int},
			                                                        		{"f_License", SortOptions.Int},
			                                                        	}

			}, new MapOnlyView(), new InMemoryRavenConfiguration());

			var perFieldAnalyzerWrapper = simpleIndex.CreateAnalyzer(new LowerCaseKeywordAnalyzer(), new List<Action>());

			var tokenStream = perFieldAnalyzerWrapper.TokenStream("f_name", new StringReader("hello Shrek"));
			while (tokenStream.IncrementToken())
			{
				var attribute = (TermAttribute) tokenStream.GetAttribute<ITermAttribute>();
				Assert.Equal("hello Shrek", attribute.Term);
			}
		}

		[Fact]
		public void UsingKeywordAnalyzing()
		{
			using(var store = NewDocumentStore())
			using (var session = store.OpenSession())
			{
				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map =
						@"from s in docs.Softs select new { s.f_platform, s.f_name, s.f_alias,s.f_License,s.f_totaldownload}",
					Analyzers =
						{
							{"f_name", typeof (KeywordAnalyzer).AssemblyQualifiedName},
							{"f_alias", typeof (KeywordAnalyzer).AssemblyQualifiedName},
						},
					Indexes =
						{
							{"f_platform", FieldIndexing.NotAnalyzed},
							{"f_License", FieldIndexing.NotAnalyzed},
							{"f_totaldownload", FieldIndexing.NotAnalyzed},
							{"f_name", FieldIndexing.Analyzed},
							{"f_alias", FieldIndexing.Analyzed},
						},
					SortOptions =
						{
							{"f_totaldownload", SortOptions.Int},
							{"f_License", SortOptions.Int},
						}

				}, true);

				Soft entity = new Soft
				{
					f_platform = 1,
					f_name = "hello Shrek",
					f_alias = "world",
					f_License = "agpl",
					f_totaldownload = -1
				};
				session.Store(entity);
				session.Advanced.GetMetadataFor(entity)["Raven-Entity-Name"] = "Softs";
				session.SaveChanges();

				List<Soft> tmps = session.Advanced.LuceneQuery<Soft>("test").
										WaitForNonStaleResults(TimeSpan.FromHours(1))
										.WhereStartsWith("f_name", "s")
										.OrderBy(new[] { "-f_License", "f_totaldownload" })
										.ToList();

				
				Assert.Empty(tmps);
			}
		}
	}
}