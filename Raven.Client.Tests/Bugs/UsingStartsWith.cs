using System.Linq;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Client.Tests.Bugs
{
    public class UsingStartsWith : LocalClientTest
    {
        [Fact]
        public void DefaultIndexingBehaviourAllowStartsWith()
        {
            using (var store = this.NewDocumentStore())
            {
                var index = new IndexDefinition<Blog, BlogTagItem>()
                {
                    Map = docs => from doc in docs
                                  from tag in doc.Tags
                                  select new
                                  {
                                      tag.Name
                                  },
                    Reduce = results => from result in results
                                        group result by result.Name into g
                                        select new
                                        {
                                            Name = g.Key,
                                            Count = g.Count()
                                        }

                }.ToIndexDefinition(store.Conventions);

                store.DatabaseCommands.PutIndex("TagInfo", index);


                using (var session = store.OpenSession())
                {
                    var newBlog = new Blog()
                    {
                        Tags = new[]{
                             new BlogTag() { Name = "SuperCallaFragalisticExpealadocious" }
                        }
                    };
                    session.Store(newBlog);
                    session.SaveChanges();

                    var result = session.Query<BlogTagItem>("TagInfo")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name.StartsWith("Su"))
                        .FirstOrDefault();

                    Assert.NotNull(result);
                }
            }
        }

        public class BlogTagItem
        {
            public string Name { get; set; }
            public int Count { get; set; }
        }

        public class Blog
        {
            public string Title
            {
                get;
                set;
            }

            public string Category
            {
                get;
                set;
            }

            public BlogTag[] Tags
            {
                get;
                set;
            }
        }

        public class BlogTag
        {
            public string Name { get; set; }
        }
    }
}