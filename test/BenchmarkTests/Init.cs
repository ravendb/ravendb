using System;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace BenchmarkTests
{
    [Collection(nameof(Init))]
    public class Init : BenchmarkTestBase
    {
        public Init(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Initialize()
        {
            using (GetDocumentStore()) // warm-up
            {
                await Task.Delay(1000);
            }

            using (var store = new DocumentStore())
            {
                store.Urls = new[] { Server.WebUrl };
                store.Initialize();

                foreach (var type in typeof(Init).Assembly.GetTypes())
                {
                    if (type.IsSubclassOf(typeof(BenchmarkTestBase)) == false)
                        continue;

                    if (type == typeof(Init))
                        continue;

                    var instance = (BenchmarkTestBase)Activator.CreateInstance(type, Output);

                    await instance.InitAsync(store);
                }
            }
        }

        public override Task InitAsync(DocumentStore store)
        {
            return Task.CompletedTask;
        }
    }
}
