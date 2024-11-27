using Confluent.Kafka;
using System;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Queue;

public class RavenDB_21659_Kafka : KafkaEtlTestBase
{
    public RavenDB_21659_Kafka(ITestOutputHelper output) : base(output)
    {
    }

    [RequiresKafkaRetryFact]
    public async Task CanPassOptionalAttributesToLoadToMethod()
    {
        using (var store = GetDocumentStore())
        {
            var config = SetupQueueEtlToKafka(store,
                @$"loadToUsers{TopicSuffix}(this, {{
                                                            dataschema: 'urn:foo:names:specification:bar:1.2.3',
                                                            Subject: 'my subject',
                                                            Time: new Date()
                                                     }})", new[] { "Users" });

            var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User
                {
                    Name = "Arek"
                });
                await session.SaveChangesAsync();
            }

            await AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

            using IConsumer<string, byte[]> consumer = CreateKafkaConsumer(new[] { $"Users{TopicSuffix}" });

            var consumeResult = consumer.Consume();
            var bytesAsString = Encoding.UTF8.GetString(consumeResult.Message.Value);

            var user = JsonConvert.DeserializeObject<User>(bytesAsString);

            Assert.NotNull(user);
            Assert.Equal(user.Name, "Arek");

            // validate headers

            consumeResult.Message.Headers.TryGetLastBytes("ce_dataschema", out var dataSchema);
            Assert.Equal("urn:foo:names:specification:bar:1.2.3", Encoding.UTF8.GetString(dataSchema));

            consumeResult.Message.Headers.TryGetLastBytes("ce_subject", out var subject);
            Assert.Equal("my subject", Encoding.UTF8.GetString(subject));

            consumeResult.Message.Headers.TryGetLastBytes("ce_time", out var date);
            Assert.NotEmpty(Encoding.UTF8.GetString(date));

            consumer.Close();
        }
    }
}
