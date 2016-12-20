using System.Collections.Generic;
using System.Linq;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4970 : RavenTestBase
    {
        [Fact]
        public void AsyncLoadWithIncludeWithSelect()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Image("images/1"));
                    session.Store(new Image("images/2"));
                    session.Store(new Image("images/3"));
                    session.Store(new TestProduct
                    {
                        Id = "products/1",
                        Title = "Test",
                        MainImageId = "images/3",
                        Images = new Dictionary<int, TestImage>
                        {
                            {
                                1, new TestImage("images/1", 1)
                            },
                            {
                                2, new TestImage("images/2", 2)
                            }
                        }
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var product = session
                        .Include<TestProduct>(p => p.MainImageId)
                        .Include<TestProduct>(p => p.Images.Select(i => i.Value.ImageId))
                        .LoadAsync<TestProduct>("products/1")
                        .Result;

                    Assert.NotNull(product);
                }
            }
        }
    }


    public class TestProduct
    {
        public string Id { get; set; }
        public string Title { get; set; }
        public string MainImageId { get; set; }
        public Dictionary<int, TestImage> Images { get; set; }
    }

    public class TestImage
    {
        public TestImage(string imageId, int index)
        {
            ImageId = imageId;
            Index = index;
        }

        public string ImageId { get; set; }
        public int Index { get; set; }
    }

    public class Image
    {
        public string Id { get; set; }
        public string Url { get; set; }

        public Image(string id)
        {
            Id = id;
        }
    }
}