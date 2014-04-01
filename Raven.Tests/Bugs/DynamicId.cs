using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs

{
	public class DynamicId : RavenTest
	{
		[Fact]
		public void AddEntity()
		{
			using (var store = NewDocumentStore())
			{
				store.Conventions.FindIdentityPropertyNameFromEntityName = typeName => "ID";
				store.Conventions.FindIdentityProperty = prop => prop.Name == "ID";

				IDocumentSession session = store.OpenSession();

				var article = new Article
				{
					Title = "Article 1",
					SubTitle = "Article 1 subtitle",
					PublishDate = DateTime.UtcNow.Add(TimeSpan.FromDays(1))
				};
				session.Store(article);
				session.SaveChanges();

				Assert.True(article.ID > 0);

				var insertedArticle = session.Query<Article>().Where(
					a => a.ID.In(new int[] {article.ID}) && a.PublishDate > DateTime.UtcNow).FirstOrDefault();

				Assert.NotNull(insertedArticle);
			}
		}

		public class Article
		{
			public int ID { get; set; }
			public string Title { get; set; }
			public string SubTitle { get; set; }
			public DateTime PublishDate { get; set; }
		}
	}
}