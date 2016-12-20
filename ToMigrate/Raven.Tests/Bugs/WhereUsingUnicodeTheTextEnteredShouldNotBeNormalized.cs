using System.Diagnostics;
using System.Linq;

using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Bugs
{
    public class WhereUsingUnicodeTheTextEnteredShouldNotBeNormalized : RavenTest
    {

        [Theory]
        /*
         * If we fail on 'Оптиматика' we need to make sure that the generated scanner code NextState() method was not overwritten by regenerating the parser.
         */
        [InlineData("Оптиматика")]
        [InlineData("לְשֵׁם יִחוּד קֻדְשָׁא בְּרִיךְ הוּא וּשְׁכִינְתֵּהּ")]
        public void WhenUsingEmbedded(string content)
        {
            using (var documentStore = NewDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new UnicodeItem { Content = content, Id = "item/1" });
                    session.Store(new UnicodeItem { Content = content, Id = "item/2" });
                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {

                    var result = session.Query<UnicodeItem>()
                        .Customize(customization => customization.WaitForNonStaleResultsAsOfLastWrite())
                        .Count(item => item.Content == content);

                    Assert.Equal(2, result);
                }
            }
        }

        [Theory]
        [InlineData("לְשֵׁם יִחוּד קֻדְשָׁא בְּרִיךְ הוּא וּשְׁכִינְתֵּהּ")]
        [InlineData("Оптиматика")]
        public void WhenUsingHttp(string content)
        {
            using (var store = NewRemoteDocumentStore(fiddler:true))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new UnicodeItem { Content = content, Id = "item/1" });
                    session.Store(new UnicodeItem { Content = content, Id = "item/2" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var result = session.Query<UnicodeItem>()
                        .Customize(customization => customization.WaitForNonStaleResultsAsOfLastWrite())
                        .Count(item => item.Content == content);
                    WaitForUserToContinueTheTest(url:store.Url);
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
