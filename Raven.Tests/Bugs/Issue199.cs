//-----------------------------------------------------------------------
// <copyright file="Issue199.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Lucene.Net.Analysis;
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class Issue199 : LocalClientTest
	{
		[Fact]
		public void CanQueryStartingInH()
		{
			using(var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					store.DatabaseCommands.PutIndex("test", new IndexDefinition
					{
						Map = @"from s in docs.Softs
						select new { s.f_platform, s.f_name, s.f_alias,s.f_License,s.f_totaldownload}",
						Analyzers =
							{
								{"f_name", typeof(KeywordAnalyzer).AssemblyQualifiedName},
								{"f_alias", typeof(KeywordAnalyzer).AssemblyQualifiedName},
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
								{ "f_totaldownload", SortOptions.Int },
								{ "f_License", SortOptions.Int },
							}

					}, true);

					var entity = new 
					{
						f_platform = 1,
						f_name = "hello",
						f_alias = "world",
						f_License = "agpl",
						f_totaldownload = -1
					};
					session.Store(entity);

					session.Advanced.GetMetadataFor(entity)["Raven-Entity-Name"] = "Softs";

					session.SaveChanges();

					Assert.NotEmpty(
						session.Advanced.LuceneQuery<dynamic>("test").
						WaitForNonStaleResults().
						Where("f_platform:1 AND (f_name:*H* OR f_alias:*H*)")
						.OrderBy(new[] { "-f_License", "f_totaldownload" })
						.ToList()
						);
				}
			}
		}
	}
}