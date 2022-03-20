using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class WhereUsingUnicodeTheTextEnteredShouldNotBeNormalized : RavenTestBase
    {
        public WhereUsingUnicodeTheTextEnteredShouldNotBeNormalized(ITestOutputHelper output) : base(output)
        {
        }


        [Theory]
        /*
         * If we fail on '??????????' we need to make sure that the generated scanner code NextState() method was not overwritten by regenerating the parser.
         */
        [InlineData("??????????")]
        [InlineData("?????? ?????? ???????? ???????? ???? ???????????????")]
        public void WhenUsingEmbedded(string content)
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new UnicodeItem { Content = content, Id = "item/1" });
                    session.Store(new UnicodeItem { Content = content, Id = "item/2" });
                    session.SaveChanges();
                }
                using (var session = documentStore.OpenSession())
                {
                    Indexes.WaitForIndexing(documentStore);
                    var result = session.Query<UnicodeItem>()
                        .Count(item => item.Content == content);

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
