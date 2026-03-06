using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using RabbitMQ.Client;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Server.Documents.QueueSink;
using Tests.Infrastructure;
using Tests.Infrastructure.ConnectionString;
using Xunit;

namespace SlowTests.Server.Documents.QueueSink;

public class RabbitMqSinkTests : RabbitMqQueueSinkTestBase
{
    public RabbitMqSinkTests(ITestOutputHelper output) : base(output)
    {
    }

    [RequiresRabbitMqRetryFact]
    public async Task SimpleScript()
    {
        var user1 = new User { Id = "users/1", FirstName = "John", LastName = "Doe" };
        var user2 = new User { Id = "users/2", FirstName = "Jane", LastName = "Smith" };

        byte[] userBytes1 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user1));
        byte[] userBytes2 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user2));

        var producer = CreateRabbitMqProducer(UsersQueueName);

        await producer.BasicPublishAsync(exchange: "", routingKey: UsersQueueName, body: new ReadOnlyMemory<byte>(userBytes1));
        await producer.BasicPublishAsync(exchange: "", routingKey: UsersQueueName, body: new ReadOnlyMemory<byte>(userBytes2));

        using var store = GetDocumentStore();
        var config = SetupRabbitMqQueueSink(store, "this['@metadata']['@collection'] = 'Users'; put(this.Id, this)",
            new List<string>() { UsersQueueName });

        var etlDone = WaitForQueueSinkBatch(store, (n, statistics) => statistics.ConsumeSuccesses >= 2);
        await AssertQueueSinkDoneAsync(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

        using var session = store.OpenSession();

        var users = session.Query<User>().ToList();
        Assert.Equal(2, users.Count);

        var fetchedUser1 = session.Load<User>("users/1");
        Assert.NotNull(fetchedUser1);
        Assert.Equal("users/1", fetchedUser1.Id);
        Assert.Equal("John", fetchedUser1.FirstName);
        Assert.Equal("Doe", fetchedUser1.LastName);

        var fetchedUser2 = session.Load<User>("users/2");
        Assert.NotNull(fetchedUser2);
        Assert.Equal("users/2", fetchedUser2.Id);
        Assert.Equal("Jane", fetchedUser2.FirstName);
        Assert.Equal("Smith", fetchedUser2.LastName);
    }

    [RequiresRabbitMqRetryFact]
    public async Task SimpleScriptMultiQueues()
    {
        var user1 = new User { Id = "users/1", FirstName = "John", LastName = "Doe" };
        var user2 = new User { Id = "users/2", FirstName = "Jane", LastName = "Smith" };
        var user3 = new User { Id = "users/3", FirstName = "Jane", LastName = "Smith" };
        var user4 = new User { Id = "users/4", FirstName = "Jane", LastName = "Smith" };

        byte[] userBytes1 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user1));
        byte[] userBytes2 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user2));
        byte[] userBytes3 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user3));
        byte[] userBytes4 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user4));

        string developersQueueName = $"developers{QueueSuffix}";

        var producer = CreateRabbitMqProducer(UsersQueueName, developersQueueName);

        await producer.BasicPublishAsync(exchange: "", routingKey: UsersQueueName, body: new ReadOnlyMemory<byte>(userBytes1));
        await producer.BasicPublishAsync(exchange: "", routingKey: UsersQueueName, body: new ReadOnlyMemory<byte>(userBytes2));

        await producer.BasicPublishAsync(exchange: "", routingKey: developersQueueName, body: new ReadOnlyMemory<byte>(userBytes3));
        await producer.BasicPublishAsync(exchange: "", routingKey: developersQueueName, body: new ReadOnlyMemory<byte>(userBytes4));

        using var store = GetDocumentStore();
        var config = SetupRabbitMqQueueSink(store, "this['@metadata']['@collection'] = 'Users'; put(this.Id, this)",
            new List<string>() { UsersQueueName, developersQueueName });

        var etlDone = WaitForQueueSinkBatch(store, (n, statistics) => statistics.ConsumeSuccesses ! >= 4);
        await AssertQueueSinkDoneAsync(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

        using var session = store.OpenSession();

        var users = session.Query<User>().ToList();
        Assert.Equal(4, users.Count);

        var fetchedUser1 = session.Load<User>("users/1");
        Assert.NotNull(fetchedUser1);
        Assert.Equal("users/1", fetchedUser1.Id);
        Assert.Equal("John", fetchedUser1.FirstName);
        Assert.Equal("Doe", fetchedUser1.LastName);

        var fetchedUser2 = session.Load<User>("users/2");
        Assert.NotNull(fetchedUser2);
        Assert.Equal("users/2", fetchedUser2.Id);
        Assert.Equal("Jane", fetchedUser2.FirstName);
        Assert.Equal("Smith", fetchedUser2.LastName);

        var fetchedUser3 = session.Load<User>("users/3");
        Assert.NotNull(fetchedUser3);
        Assert.Equal("users/3", fetchedUser3.Id);
        Assert.Equal("Jane", fetchedUser3.FirstName);
        Assert.Equal("Smith", fetchedUser3.LastName);

        var fetchedUser4 = session.Load<User>("users/4");
        Assert.NotNull(fetchedUser4);
        Assert.Equal("users/4", fetchedUser4.Id);
        Assert.Equal("Jane", fetchedUser4.FirstName);
        Assert.Equal("Smith", fetchedUser4.LastName);
    }

    [RequiresRabbitMqRetryFact]
    public async Task ComplexScript()
    {
        var script =
            @"var item = { Id : this.Id, FirstName : this.FirstName, LastName : this.LastName, FullName : this.FirstName + ' ' + this.LastName }
                 put(this.Id, item)";

        var user1 = new User { Id = "users/1", FirstName = "John", LastName = "Doe" };
        var user2 = new User { Id = "users/2", FirstName = "Jane", LastName = "Smith" };

        byte[] userBytes1 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user1));
        byte[] userBytes2 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user2));

        var producer = CreateRabbitMqProducer(UsersQueueName);

        await producer.BasicPublishAsync(exchange: "", routingKey: UsersQueueName, body: new ReadOnlyMemory<byte>(userBytes1));
        await producer.BasicPublishAsync(exchange: "", routingKey: UsersQueueName, body: new ReadOnlyMemory<byte>(userBytes2));

        using var store = GetDocumentStore();
        var config = SetupRabbitMqQueueSink(store, script, new List<string>() { UsersQueueName });

        var etlDone = WaitForQueueSinkBatch(store, (n, statistics) => statistics.ConsumeSuccesses >= 2);
        await AssertQueueSinkDoneAsync(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

        using var session = store.OpenSession();

        var fetchedUser1 = session.Load<User>("users/1");
        Assert.NotNull(fetchedUser1);
        Assert.Equal("users/1", fetchedUser1.Id);
        Assert.Equal("John", fetchedUser1.FirstName);
        Assert.Equal("Doe", fetchedUser1.LastName);
        Assert.Equal("John Doe", fetchedUser1.FullName);

        var fetchedUser2 = session.Load<User>("users/2");
        Assert.NotNull(fetchedUser2);
        Assert.Equal("users/2", fetchedUser2.Id);
        Assert.Equal("Jane", fetchedUser2.FirstName);
        Assert.Equal("Smith", fetchedUser2.LastName);
        Assert.Equal("Jane Smith", fetchedUser2.FullName);
    }

    [RequiresRabbitMqRetryFact]
    public async Task SimpleScriptMultipleInserts()
    {
        var numberOfUsers = 10;

        var producer = CreateRabbitMqProducer(UsersQueueName);

        for (int i = 0; i < numberOfUsers; i++)
        {
            var user = new User { Id = $"users/{i}", FirstName = $"firstname{i}", LastName = $"lastname{i}" };
            byte[] userBytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user));
            await producer.BasicPublishAsync(exchange: "", routingKey: UsersQueueName, body: new ReadOnlyMemory<byte>(userBytes));
        }

        using var store = GetDocumentStore();
        var config = SetupRabbitMqQueueSink(store, "this['@metadata']['@collection'] = 'Users'; put(this.Id, this)",
            new List<string>() { UsersQueueName });

        var etlDone = WaitForQueueSinkBatch(store, (n, statistics) => statistics.ConsumeSuccesses >= numberOfUsers);
        await AssertQueueSinkDoneAsync(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

        using var session = store.OpenSession();

        var users = session.Query<User>().ToList();
        Assert.Equal(numberOfUsers, users.Count);

        for (int i = 0; i < numberOfUsers; i++)
        {
            var fetchedUser = session.Load<User>($"users/{i}");
            Assert.NotNull(fetchedUser);
            Assert.Equal($"users/{i}", fetchedUser.Id);
            Assert.Equal($"firstname{i}", fetchedUser.FirstName);
            Assert.Equal($"lastname{i}", fetchedUser.LastName);
        }
    }

    [RequiresRabbitMqRetryFact]
    public void Error_if_script_is_empty()
    {
        var config = new QueueSinkConfiguration
        {
            Name = "test",
            ConnectionStringName = "test",
            BrokerType = QueueBrokerType.RabbitMq,
            Scripts = { new QueueSinkScript { Name = "test", Script = @"" } }
        };

        config.Initialize(new QueueConnectionString
        {
            Name = "Foo",
            BrokerType = QueueBrokerType.RabbitMq,
            RabbitMqConnectionSettings = new RabbitMqConnectionSettings
            {
                ConnectionString = RabbitMqConnectionString.Instance.VerifiedUrl.Value
            }
        });

        List<string> errors;
        config.Validate(out errors);

        Assert.Equal(1, errors.Count);

        Assert.Equal("Script 'test' must not be empty", errors[0]);
    }

    [RequiresRabbitMqRetryFact]
    public async Task ConnectionStringChanges_WillRestartRabbitMqSink()
    {
        var user1 = new User { Id = "users/1", FirstName = "John", LastName = "Doe" };
        var user2 = new User { Id = "users/2", FirstName = "Jane", LastName = "Smith" };

        byte[] userBytes1 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user1));
        byte[] userBytes2 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user2));

        var producer = CreateRabbitMqProducer(UsersQueueName);

        await producer.BasicPublishAsync(exchange: "", routingKey: UsersQueueName, body: new ReadOnlyMemory<byte>(userBytes1));
        await producer.BasicPublishAsync(exchange: "", routingKey: UsersQueueName, body: new ReadOnlyMemory<byte>(userBytes2));

        using var store = GetDocumentStore();
        var config = SetupRabbitMqQueueSink(store, "this['@metadata']['@collection'] = 'Users'; put(this.Id, this)",
            new List<string>() { UsersQueueName });

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var mre = new ManualResetEventSlim();
        database.QueueSinkLoader.ProcessRemoved += process => mre.Set();

        var rabbitMqQueueSink = (RabbitMqQueueSink)database.QueueSinkLoader.Processes.SingleOrDefault();

        store.Maintenance.Send(new PutConnectionStringOperation<QueueConnectionString>(new QueueConnectionString
        {
            Name = config.ConnectionStringName,
            BrokerType = QueueBrokerType.RabbitMq,
            RabbitMqConnectionSettings = new RabbitMqConnectionSettings { ConnectionString = RabbitMqConnectionString.Instance.VerifiedUrl.Value + "/" }
        }));

        Assert.True(mre.Wait(TimeSpan.FromSeconds(10)));

        var rabbitMqQueueSink2 = (RabbitMqQueueSink)database.QueueSinkLoader.Processes.SingleOrDefault();

        Assert.NotEqual(rabbitMqQueueSink, rabbitMqQueueSink2);
        Assert.False(rabbitMqQueueSink.Configuration.Connection.IsEqual(rabbitMqQueueSink2.Configuration.Connection));
    }
}
