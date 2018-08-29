using System.Threading.Tasks;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_9479 : RavenTestBase
    {
        [Theory]
        [InlineData((char)0)]
        [InlineData((char)1)]
        [InlineData((char)16)]
        [InlineData((char)32)]
        [InlineData((char)127)]
        [InlineData((char)159)]
        [InlineData((char)114)]
        public async Task GetStoreAndGetChar(char ch)
        {
            using (var store = GetDocumentStore())
            {
                string documentId;
                using (var session = store.OpenAsyncSession())
                {
                    var question = new Question
                    {
                        Letter = ch
                    };

                    await session.StoreAsync(question);
                    await session.SaveChangesAsync();

                    documentId = question.Id;
                }

                WaitForUserToContinueTheTest(store);

                using (var session = store.OpenAsyncSession())
                {
                    var question = await session.LoadAsync<Question>(documentId);
                    Assert.Equal(ch, question.Letter);
                }
            }
        }

        public class Question
        {
            public string Id { get; set; }

            public char Letter { get; set; }
        }
    }
}
