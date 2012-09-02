//-----------------------------------------------------------------------
// <copyright file="ThrowingAnalyzer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using Lucene.Net.Analysis;
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs.Indexing
{
	public class ThrowingAnalyzer : RavenTest
	{
		[Fact]
		public void Should_give_clear_error()
		{
			using(var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("foo",
												new IndexDefinition
												{
													Map = "from doc in docs select new { doc.Name}",
													Analyzers = { { "Name", typeof(ThrowingAnalyzerImpl).AssemblyQualifiedName } }
												});

				using(var session = store.OpenSession())
				{
					session.Store(new User{Name="Ayende"});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					session.Query<User>("foo")
						.Customize(x => x.WaitForNonStaleResults())
						.ToList();

				}
				Assert.NotEmpty(store.DocumentDatabase.Statistics.Errors);
			}
		}

		public class ThrowingAnalyzerImpl : Analyzer
		{
			public ThrowingAnalyzerImpl()
			{
				throw new InvalidOperationException("opps");
			}

			public override TokenStream TokenStream(string fieldName, TextReader reader)
			{
				throw new NotImplementedException();
			}
		}
	}
}
