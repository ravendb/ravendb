// -----------------------------------------------------------------------
//  <copyright file="DyanmicId.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class DyanmicId : RavenTestBase
    {
        public DyanmicId(ITestOutputHelper output) : base(output)
        {
        }

        private class Article
        {
            public string ID { get; set; }
            public string Title { get; set; }
            public string SubTitle { get; set; }
            public DateTime PublishDate { get; set; }
        }

        [Fact]
        public void AddEntity()
        {
            //SetUp
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.FindIdentityPropertyNameFromCollectionName = (typeName) => "ID";
                    s.Conventions.FindIdentityProperty = prop => prop.Name == "ID";
                }
            }))
            {
                using (var session = store.OpenSession())
                {

                    var article = new Article()
                    {
                        Title = "Article 1",
                        SubTitle = "Article 1 subtitle",
                        PublishDate = DateTime.UtcNow.Add(TimeSpan.FromDays(1))
                    };
                    session.Store(article);
                    session.SaveChanges();

                    Assert.NotNull(article.ID);

                    var insertedArticle = session.Query<Article>().Where(
                        a => a.ID.In(new string[] { article.ID }) && a.PublishDate > DateTime.UtcNow).FirstOrDefault();

                    Assert.NotNull(insertedArticle);
                }
            }
        }

    }
}
