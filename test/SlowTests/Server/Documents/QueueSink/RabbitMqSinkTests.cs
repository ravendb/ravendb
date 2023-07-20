using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using RabbitMQ.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.QueueSink;
using Tests.Infrastructure;
using Tests.Infrastructure.ConnectionString;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.QueueSink;

public class RabbitMqSinkTests : QueueSinkTestBase
{
    public RabbitMqSinkTests(ITestOutputHelper output) : base(output)
    {
    }

    private const string DefaultScript = "put(this.Id, this)";
    private readonly List<string> _defaultQueue = new() { "users" };

    [RequiresRabbitMqRetryFact]
    public void SimpleScript()
    {
        var user1 = new User { Id = "users/1", FirstName = "John", LastName = "Doe" };
        var user2 = new User { Id = "users/2", FirstName = "Jane", LastName = "Smith" };

        byte[] userBytes1 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user1));
        byte[] userBytes2 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user2));

        var producer = CreateRabbitMqProducer("amqp://guest:guest@localhost:5672/");

        producer.QueueDeclare(queue: _defaultQueue[0], exclusive: false);

        producer.BasicPublish(exchange: "", routingKey: _defaultQueue[0], basicProperties: null,
            body: new ReadOnlyMemory<byte>(userBytes1));
        producer.BasicPublish(exchange: "", routingKey: _defaultQueue[0], basicProperties: null,
            body: new ReadOnlyMemory<byte>(userBytes2));

        using var store = GetDocumentStore();
        SetupRabbitMqQueueSink(store, "this['@metadata']['@collection'] = 'Users'; put(this.Id, this)",
            new List<string>() { "users" });

        var etlDone = WaitForEtl(store, (n, statistics) => statistics.ConsumeSuccesses != 0);
        AssertQueueSinkDone(etlDone, TimeSpan.FromMinutes(1));

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
    public void SimpleScriptMultiQueues()
    {
        var user1 = new User { Id = "users/1", FirstName = "John", LastName = "Doe" };
        var user2 = new User { Id = "users/2", FirstName = "Jane", LastName = "Smith" };
        var user3 = new User { Id = "users/3", FirstName = "Jane", LastName = "Smith" };
        var user4 = new User { Id = "users/4", FirstName = "Jane", LastName = "Smith" };

        byte[] userBytes1 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user1));
        byte[] userBytes2 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user2));
        byte[] userBytes3 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user3));
        byte[] userBytes4 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user4));

        var producer = CreateRabbitMqProducer("amqp://guest:guest@localhost:5672/");

        producer.QueueDeclare(queue: _defaultQueue[0], exclusive: false);
        producer.QueueDeclare(queue: "developers", exclusive: false);

        producer.BasicPublish(exchange: "", routingKey: _defaultQueue[0], basicProperties: null,
            body: new ReadOnlyMemory<byte>(userBytes1));
        producer.BasicPublish(exchange: "", routingKey: _defaultQueue[0], basicProperties: null,
            body: new ReadOnlyMemory<byte>(userBytes2));
        
        producer.BasicPublish(exchange: "", routingKey: "developers", basicProperties: null,
            body: new ReadOnlyMemory<byte>(userBytes3));
        producer.BasicPublish(exchange: "", routingKey: "developers", basicProperties: null,
            body: new ReadOnlyMemory<byte>(userBytes4));

        using var store = GetDocumentStore();
        SetupRabbitMqQueueSink(store, "this['@metadata']['@collection'] = 'Users'; put(this.Id, this)",
            new List<string>() { "users", "developers" });

        var etlDone = WaitForEtl(store, (n, statistics) => statistics.ConsumeSuccesses != 0);
        AssertQueueSinkDone(etlDone, TimeSpan.FromMinutes(1));

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
    public void ComplexScript()
    {
        var script =
            @"var item = { Id : this.Id, FirstName : this.FirstName, LastName : this.LastName, FullName : this.FirstName + ' ' + this.LastName }
                 put(this.Id, item)";

        var user1 = new User { Id = "users/1", FirstName = "John", LastName = "Doe" };
        var user2 = new User { Id = "users/2", FirstName = "Jane", LastName = "Smith" };

        byte[] userBytes1 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user1));
        byte[] userBytes2 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user2));

        var producer = CreateRabbitMqProducer("amqp://guest:guest@localhost:5672/");

        producer.QueueDeclare(queue: _defaultQueue[0], exclusive: false);

        producer.BasicPublish(exchange: "", routingKey: _defaultQueue[0], basicProperties: null,
            body: new ReadOnlyMemory<byte>(userBytes1));
        producer.BasicPublish(exchange: "", routingKey: _defaultQueue[0], basicProperties: null,
            body: new ReadOnlyMemory<byte>(userBytes2));

        using var store = GetDocumentStore();
        SetupRabbitMqQueueSink(store, script, new List<string>() { "users" });

        var etlDone = WaitForEtl(store, (n, statistics) => statistics.ConsumeSuccesses != 0);
        AssertQueueSinkDone(etlDone, TimeSpan.FromSeconds(20));

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
    public void SimpleScriptMultipleInserts()
    {
        var numberOfUsers = 10;

        var producer = CreateRabbitMqProducer("amqp://guest:guest@localhost:5672/");

        for (int i = 0; i < numberOfUsers; i++)
        {
            var user = new User { Id = $"users/{i}", FirstName = $"firstname{i}", LastName = $"lastname{i}" };
            byte[] userBytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user));
            producer.BasicPublish(exchange: "", routingKey: _defaultQueue[0], basicProperties: null,
                body: new ReadOnlyMemory<byte>(userBytes));
        }

        using var store = GetDocumentStore();
        SetupRabbitMqQueueSink(store, "this['@metadata']['@collection'] = 'Users'; put(this.Id, this)",
            new List<string>() { "users" });

        var etlDone = WaitForEtl(store, (n, statistics) => statistics.ConsumeSuccesses != 0);
        AssertQueueSinkDone(etlDone, TimeSpan.FromMinutes(1));

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

    [Fact]
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
                ConnectionString = RabbitMqConnectionString.Instance.VerifiedConnectionString.Value
            }
        });

        List<string> errors;
        config.Validate(out errors);

        Assert.Equal(1, errors.Count);

        Assert.Equal("Script 'test' must not be empty", errors[0]);
    }

    private void SetupRabbitMqQueueSink(DocumentStore store, string script, List<string> queues,
        string configurationName = null, string transformationName = null)
    {
        var connectionStringName = $"{store.Database} to Kafka";

        QueueSinkScript queueSinkScript = new QueueSinkScript
        {
            Name = transformationName ?? $"Queue Sink : {connectionStringName}",
            Queues = new List<string>(queues),
            Script = script,
        };
        var config = new QueueSinkConfiguration
        {
            Name = configurationName ?? connectionStringName,
            ConnectionStringName = connectionStringName,
            Scripts = { queueSinkScript },
            BrokerType = QueueBrokerType.RabbitMq
        };

        AddQueueSink(store, config,
            new QueueConnectionString
            {
                Name = connectionStringName,
                BrokerType = QueueBrokerType.RabbitMq,
                RabbitMqConnectionSettings = new RabbitMqConnectionSettings
                {
                    ConnectionString = "amqp://guest:guest@localhost:5672/"
                }
            });
    }

    private IModel CreateRabbitMqProducer(string url)
    {
        var connectionFactory = new ConnectionFactory { Uri = new Uri(url) };
        var connection = connectionFactory.CreateConnection();
        var producer = connection.CreateModel();
        return producer;
    }
}
