using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Xunit;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Client.Linq;

namespace Raven.Tests.Bugs.Indexing
{
	public class CanIndexWithCharLiteral : RavenTest
	{
		[Fact]
		public void CanQueryDocumentsIndexWithCharLiteral()
		{
			using (var store = NewDocumentStore()) {
				store.DatabaseCommands.PutIndex("test", new IndexDefinition {
					Map = "from doc in docs select  new { SortVersion = doc.Version.PadLeft(5, '0') }",
					Stores = new[] { new { Field = "SortVersion", Storage = FieldStorage.Yes } }.ToDictionary(d => d.Field, d => d.Storage)
				});

				using (var s = store.OpenSession()) {
					var entity = new { Version = "1" };
					s.Store(entity);
					s.SaveChanges();
				}

				using (var s = store.OpenSession()) {
					Assert.Equal(1, s.Query<object>("test").Customize(x => x.WaitForNonStaleResults()).Count());
					Assert.Equal("00001", s.Query<object>("test").Customize(x => x.WaitForNonStaleResults()).AsProjection<Result>().First().SortVersion);
				}
			}
		}
	}

	public class Result
	{
		public string SortVersion { get; set; }
	}
}