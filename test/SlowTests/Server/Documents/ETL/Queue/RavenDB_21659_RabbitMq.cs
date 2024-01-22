using System;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using RabbitMQ.Client;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Queue;

public class RavenDB_21659_RabbitMq : RabbitMqEtlTestBase
{
    public RavenDB_21659_RabbitMq(ITestOutputHelper output) : base(output)
    {
    }

    [RequiresRabbitMqRetryFact]
    public void CanPassOptionalAttributesToLoadToMethod()
    {
        using (var store = GetDocumentStore())
        {
            var config = SetupQueueEtlToRabbitMq(store,
                @$"loadToUsers{ExchangeSuffix}(this, {{
                                                            DataSchema: 'http://www.my-app.com',
                                                            subject: id(this),
                                                            time: '2023-11-14T14:55:00.0000000',
                                                     }})", new[] { "Users" });

            var etlDone = WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Arek" }, "users/1");
                session.SaveChanges();
            }

            AssertEtlDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

            using var channel = CreateRabbitMqChannel();
            var consumer = new TestRabbitMqConsumer(channel);

            channel.BasicConsume(queue: $"Users{ExchangeSuffix}", autoAck: true, consumer: consumer);

            var ea = consumer.Consume();

            var body = ea.Body.ToArray();
            var bytesAsString = Encoding.UTF8.GetString(body);

            var user = JsonConvert.DeserializeObject<UserData>(bytesAsString);

            Assert.NotNull(user);
            Assert.Equal(user.Name, "Arek");

            var headers = ea.Properties.Headers;

            Assert.Equal("http://www.my-app.com/", Encoding.UTF8.GetString((byte[])headers["cloudEvents:dataschema"]));
            Assert.Equal("users/1", Encoding.UTF8.GetString((byte[])headers["cloudEvents:subject"]));
            Assert.NotEmpty(Encoding.UTF8.GetString((byte[])headers["cloudEvents:time"]));
        }
    }

    private class UserData
    {
        public string UserId { get; set; }
        public string Name { get; set; }
    }
}
