using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using RabbitMQ.Client;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.QueueSink.Issues;

public class RavenDB_22105 : RabbitMqQueueSinkTestBase
{
    public RavenDB_22105(ITestOutputHelper output) : base(output)
    {
    }

    [RequiresRabbitMqRetryFact]
    public async Task Queue_sink_process_will_prevent_from_unloading_idle_database()
    {
        DoNotReuseServer();

        var landlord = Server.ServerStore.DatabasesLandlord;

        using var store = GetDocumentStore(new Options
        {
            RunInMemory = false // in memory databases aren't unloaded
        });

        var user1 = new User { Id = "users/1", FirstName = "John", LastName = "Doe" };

        byte[] userBytes1 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user1));

        var producer = CreateRabbitMqProducer(UsersQueueName);

        producer.BasicPublish(exchange: "", routingKey: UsersQueueName, basicProperties: null,
            body: new ReadOnlyMemory<byte>(userBytes1));

        var config = SetupRabbitMqQueueSink(store, "this['@metadata']['@collection'] = 'Users'; put(this.Id, this)",
            new List<string>() { UsersQueueName });

        var etlDone = WaitForQueueSinkBatch(store, (n, statistics) => statistics.ConsumeSuccesses >= 1);
        AssertQueueSinkDone(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

        using (var session = store.OpenSession())
        {
            var users = session.Query<User>().ToList();
            Assert.Equal(1, users.Count);

            var fetchedUser1 = session.Load<User>("users/1");
            Assert.NotNull(fetchedUser1);
            Assert.Equal("users/1", fetchedUser1.Id);
            Assert.Equal("John", fetchedUser1.FirstName);
            Assert.Equal("Doe", fetchedUser1.LastName);
        }

        // Try to idle a database

        landlord.SkipShouldContinueDisposeCheck = true;

        var database = await GetDatabase(store.Database);
        database.ResetIdleTime();

        landlord.LastRecentlyUsed.AddOrUpdate(database.Name, DateTime.MinValue, (_, time) => DateTime.MinValue);

        foreach (var env in database.GetAllStoragesEnvironment())
            env.Environment.ResetLastWorkTime();

        database.LastAccessTime = DateTime.MinValue;

        Server.ServerStore.IdleOperations(null);

        Assert.Equal(0, Server.ServerStore.IdleDatabases.Count); // active queue sink process must prevent from unloading a database
        
        Assert.False(producer.IsClosed);

        var user2 = new User { Id = "users/2", FirstName = "Jane", LastName = "Smith" };
        byte[] userBytes2 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user2));
        producer.BasicPublish(exchange: "", routingKey: UsersQueueName, basicProperties: null,
            body: new ReadOnlyMemory<byte>(userBytes2));

        var etlDone2 = WaitForQueueSinkBatch(store, (n, statistics) => statistics.ConsumeSuccesses >= 1);

        AssertQueueSinkDone(etlDone2, TimeSpan.FromMinutes(1), store.Database, config);

        using (var session = store.OpenSession())
        {
            Assert.Equal(2, session.Query<User>().ToList().Count);

            var fetchedUser2 = session.Load<User>("users/2");
            Assert.NotNull(fetchedUser2);
            Assert.Equal("users/2", fetchedUser2.Id);
            Assert.Equal("Jane", fetchedUser2.FirstName);
            Assert.Equal("Smith", fetchedUser2.LastName);
        }
    }
}
