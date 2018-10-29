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
                using (var session = store.OpenAsyncSession())
                {
                    var question = await session.LoadAsync<Question>(documentId);
                    Assert.Equal(ch, question.Letter);
                }
            }
        }

        [Fact]
        public async Task GetStoreAndGetControlChar()
        {
            using (var store = GetDocumentStore())
            {
                string documentId;
                using (var session = store.OpenAsyncSession())
                {
                    var question = new Question
                    {
                        Letter = '\0',
                        Name = "Hello \0 World"
                    };

                    await session.StoreAsync(question);
                    await session.SaveChangesAsync();

                    documentId = question.Id;
                }


                using (var session = store.OpenAsyncSession())
                {
                    var question = await session.LoadAsync<Question>(documentId);
                    Assert.Equal('\0', question.Letter);
                    Assert.Equal("Hello \0 World", question.Name);
                }
            }
        }

        public class Question
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public char Letter { get; set; }
        }
    }
}
