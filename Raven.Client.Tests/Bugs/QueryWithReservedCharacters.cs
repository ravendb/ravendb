using System;
using System.Linq;
using Raven.Client.Client;
using Raven.Client.Document;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Client.Tests.Bugs
{
	public class QueryWithReservedCharacters : BaseClientTest
	{
		[Fact]
		public void WhenQueryingByGenericClrTypes_ThenAutoQuotedLuceneQueryFails()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex(
					"ByClr",
					new IndexDefinition
					{
						Map = @"from doc in docs select new { ClrType = doc[""@metadata""][""Raven-Clr-Type""] }",
						Indexes = {{"ClrType", FieldIndexing.NotAnalyzed}},
					},
					true);

				using (var session = store.OpenSession())
				{
					session.Store(new Bar<Foo> {Value = "foo"});

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var typeName = ReflectionUtil.GetFullNameWithoutVersionInformation(typeof (Bar<Foo>));
					var allSync = session
						.LuceneQuery<Bar<Foo>>("ByClr")
						.Where("ClrType:[[" + RavenQuery.Escape(typeName) + "]]")
						.WaitForNonStaleResultsAsOfNow(TimeSpan.MaxValue)
						.ToList();

					Assert.Equal(1, allSync.Count);
				}
			}
		}

		[Fact]
		public void CanQueryWithReservedCharactersWithoutException()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.LuceneQuery<object>("Raven/DocumentsByEntityName")
						.Where(RavenQuery.Escape("foo]]]]"))
						.ToList();
				}
			}
		}

		#region Nested type: Bar

		public class Bar<T>
		{
			public string Value { get; set; }
		}

		#endregion

		#region Nested type: Foo

		public class Foo
		{
		}

		#endregion
	}
}