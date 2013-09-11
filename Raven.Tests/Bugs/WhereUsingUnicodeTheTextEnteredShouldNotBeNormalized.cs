using System.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class WhereUsingUnicodeTheTextEnteredShouldNotBeNormalized : RavenTest
	{
		private const string Content = "לְשֵׁם יִחוּד קֻדְשָׁא בְּרִיךְ הוּא וּשְׁכִינְתֵּהּ";

		[Fact]
		public void WhenUsingEmbedded()
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
			using (var store = NewRemoteDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new UnicodeItem { Content = Content, Id = "item/1" });
					session.Store(new UnicodeItem { Content = Content, Id = "item/2" });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var result = session.Query<UnicodeItem>()
						.Customize(customization => customization.WaitForNonStaleResultsAsOfLastWrite())
						.Count(item => item.Content == Content);

					Assert.Equal(2, result);
				}
			}
		}

		private class UnicodeItem
		{
			public string Id { get; set; }
			public string Content { get; set; }
		}
	}
}