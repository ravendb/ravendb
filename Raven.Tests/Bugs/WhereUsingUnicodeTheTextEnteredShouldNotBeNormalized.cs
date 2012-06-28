using Raven.Client.Document;
using Raven.Database.Extensions;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class WhereUsingUnicodeTheTextEnteredShouldNotBeNormalized : RavenTest
	{
		private const string Content = "לְשֵׁם יִחוּד קֻדְשָׁא בְּרִיךְ הוּא וּשְׁכִינְתֵּהּ";

		private readonly string path;

		public WhereUsingUnicodeTheTextEnteredShouldNotBeNormalized()
		{
			path = GetType().Name;
		}

		[Fact]
		public void WhenUsingEmbadded()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new UnicodeItem { Content = Content, Id = "item/1" });
					session.Store(new UnicodeItem { Content = Content, Id = "item/2" });
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var result = session.Query<UnicodeItem>()
						.Customize(customization => customization.WaitForNonStaleResultsAsOfLastWrite())
						.Count(item => item.Content == Content);

					Assert.Equal(2, result);
				}
			}
		}

		[Fact]
		public void WhenUsingHttp()
		{
			using (GetNewServer(8079, path))
			using (var documentStore = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new UnicodeItem { Content = Content, Id = "item/1" });
					session.Store(new UnicodeItem { Content = Content, Id = "item/2" });
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var result = session.Query<UnicodeItem>()
						.Customize(customization => customization.WaitForNonStaleResultsAsOfLastWrite())
						.Count(item => item.Content == Content);

					Assert.Equal(2, result);
				}
			}
		}

		public override void Dispose()
		{
			IOExtensions.DeleteDirectory(path);
			base.Dispose();
		}

		private class UnicodeItem
		{
			public string Id { get; set; }
			public string Content { get; set; }
		}
	}
}