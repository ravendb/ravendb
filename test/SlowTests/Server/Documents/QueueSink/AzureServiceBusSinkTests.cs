using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Server.Documents.QueueSink;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.QueueSink;

public class AzureServiceBusSinkTests : AzureServiceBusQueueSinkTestBase
{
    public AzureServiceBusSinkTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Sinks, AzureServiceBusRequired = true)]
    public async Task SimpleScript()
    {
        var user1 = new User { Id = "users/1", FirstName = "John", LastName = "Doe" };
        var user2 = new User { Id = "users/2", FirstName = "Jane", LastName = "Smith" };

        byte[] userBytes1 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user1));
        byte[] userBytes2 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user2));

        var client = CreateAzureServiceBusClient();
        var sender = await CreateAzureServiceBusProducerAsync(client, UsersQueueName);

        await sender.SendMessageAsync(new ServiceBusMessage(userBytes1));
        await sender.SendMessageAsync(new ServiceBusMessage(userBytes2));

        using var store = GetDocumentStore();
        var sinkDone = WaitForQueueSinkBatch(store, (n, statistics) => statistics.ConsumeSuccesses >= 2);
        var config = SetupAzureServiceBusQueueSink(store, "this['@metadata']['@collection'] = 'Users'; put(this.Id, this)",
            new List<string> { UsersQueueName });

        await AssertQueueSinkDoneAsync(sinkDone, TimeSpan.FromMinutes(1), store.Database, config);

        using var session = store.OpenSession();

        var users = session.Query<User>().ToList();
        Assert.Equal(2, users.Count);

        var fetchedUser1 = session.Load<User>("users/1");
        Assert.NotNull(fetchedUser1);
        Assert.Equal("John", fetchedUser1.FirstName);
        Assert.Equal("Doe", fetchedUser1.LastName);

        var fetchedUser2 = session.Load<User>("users/2");
        Assert.NotNull(fetchedUser2);
        Assert.Equal("Jane", fetchedUser2.FirstName);
        Assert.Equal("Smith", fetchedUser2.LastName);
    }

    [RavenFact(RavenTestCategory.Sinks, AzureServiceBusRequired = true)]
    public async Task ComplexScript()
    {
        var script =
            @"var item = { Id : this.Id, FirstName : this.FirstName, LastName : this.LastName, FullName : this.FirstName + ' ' + this.LastName };
              put(this.Id, item);";

        var user1 = new User { Id = "users/1", FirstName = "John", LastName = "Doe" };
        var user2 = new User { Id = "users/2", FirstName = "Jane", LastName = "Smith" };

        byte[] userBytes1 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user1));
        byte[] userBytes2 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user2));

        var client = CreateAzureServiceBusClient();
        var sender = await CreateAzureServiceBusProducerAsync(client, UsersQueueName);

        await sender.SendMessageAsync(new ServiceBusMessage(userBytes1));
        await sender.SendMessageAsync(new ServiceBusMessage(userBytes2));

        using var store = GetDocumentStore();
        var sinkDone = WaitForQueueSinkBatch(store, (n, statistics) => statistics.ConsumeSuccesses >= 2);
        var config = SetupAzureServiceBusQueueSink(store, script, new List<string> { UsersQueueName });

        await AssertQueueSinkDoneAsync(sinkDone, TimeSpan.FromMinutes(1), store.Database, config);

        using var session = store.OpenSession();

        var fetchedUser1 = session.Load<User>("users/1");
        Assert.NotNull(fetchedUser1);
        Assert.Equal("John Doe", fetchedUser1.FullName);

        var fetchedUser2 = session.Load<User>("users/2");
        Assert.NotNull(fetchedUser2);
        Assert.Equal("Jane Smith", fetchedUser2.FullName);
    }

    [RavenFact(RavenTestCategory.Sinks, AzureServiceBusRequired = true)]
    public async Task MultipleMessages()
    {
        const int numberOfUsers = 10;

        var client = CreateAzureServiceBusClient();
        var sender = await CreateAzureServiceBusProducerAsync(client, UsersQueueName);

        for (int i = 0; i < numberOfUsers; i++)
        {
            var user = new User { Id = $"users/{i}", FirstName = $"firstname{i}", LastName = $"lastname{i}" };
            byte[] userBytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user));
            await sender.SendMessageAsync(new ServiceBusMessage(userBytes));
        }

        using var store = GetDocumentStore();
        var sinkDone = WaitForQueueSinkBatch(store, (n, statistics) => statistics.ConsumeSuccesses >= numberOfUsers);
        var config = SetupAzureServiceBusQueueSink(store, "this['@metadata']['@collection'] = 'Users'; put(this.Id, this)",
            new List<string> { UsersQueueName });

        await AssertQueueSinkDoneAsync(sinkDone, TimeSpan.FromMinutes(2), store.Database, config);

        using var session = store.OpenSession();

        var users = session.Query<User>().ToList();
        Assert.Equal(numberOfUsers, users.Count);

        for (int i = 0; i < numberOfUsers; i++)
        {
            var fetchedUser = session.Load<User>($"users/{i}");
            Assert.NotNull(fetchedUser);
            Assert.Equal($"firstname{i}", fetchedUser.FirstName);
            Assert.Equal($"lastname{i}", fetchedUser.LastName);
        }
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void Error_if_script_is_empty()
    {
        var config = new QueueSinkConfiguration
        {
            Name = "test",
            ConnectionStringName = "test",
            BrokerType = QueueBrokerType.AzureServiceBus,
            Scripts = { new QueueSinkScript { Name = "test", Script = string.Empty } }
        };

        config.Initialize(new QueueConnectionString
        {
            Name = "Foo",
            BrokerType = QueueBrokerType.AzureServiceBus,
            AzureServiceBusConnectionSettings = new AzureServiceBusConnectionSettings
            {
                ConnectionString = "Endpoint=sb://example.servicebus.windows.net/;SharedAccessKeyName=fake;SharedAccessKey=ZmFrZQ=="
            }
        });

        config.Validate(out var errors);

        Assert.Equal(1, errors.Count);
        Assert.Equal("Script 'test' must not be empty", errors[0]);
    }

    [RavenFact(RavenTestCategory.Sinks, AzureServiceBusRequired = true)]
    public async Task SubscriptionSource()
    {
        var topicName = $"events-{QueueSuffix}";
        const string subscriptionName = "ravendb-sub";

        var user1 = new User { Id = "users/1", FirstName = "John", LastName = "Doe" };
        var user2 = new User { Id = "users/2", FirstName = "Jane", LastName = "Smith" };

        var userBytes1 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user1));
        var userBytes2 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user2));

        var client = CreateAzureServiceBusClient();
        var sender = await CreateAzureServiceBusTopicProducerAsync(client, topicName, subscriptionName);

        await sender.SendMessageAsync(new ServiceBusMessage(userBytes1));
        await sender.SendMessageAsync(new ServiceBusMessage(userBytes2));

        using var store = GetDocumentStore();
        var sinkDone = WaitForQueueSinkBatch(store, (n, statistics) => statistics.ConsumeSuccesses >= 2);
        var config = SetupAzureServiceBusQueueSink(store, "this['@metadata']['@collection'] = 'Users'; put(this.Id, this)",
            new List<string> { AzureServiceBusSinkSource.Subscription(topicName, subscriptionName) });

        await AssertQueueSinkDoneAsync(sinkDone, TimeSpan.FromMinutes(1), store.Database, config);

        using var session = store.OpenSession();

        var users = session.Query<User>().ToList();
        Assert.Equal(2, users.Count);
        Assert.NotNull(session.Load<User>("users/1"));
        Assert.NotNull(session.Load<User>("users/2"));
    }

    [RavenFact(RavenTestCategory.Sinks, AzureServiceBusRequired = true)]
    public async Task MixedQueueAndSubscriptionSources()
    {
        var topicName = $"events-{QueueSuffix}";
        const string subscriptionName = "ravendb-sub";

        var queueUser = new User { Id = "users/1", FirstName = "Queue", LastName = "User" };
        var topicUser = new User { Id = "users/2", FirstName = "Topic", LastName = "User" };

        var client = CreateAzureServiceBusClient();

        var queueSender = await CreateAzureServiceBusProducerAsync(client, UsersQueueName);
        await queueSender.SendMessageAsync(new ServiceBusMessage(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(queueUser))));

        var topicSender = await CreateAzureServiceBusTopicProducerAsync(client, topicName, subscriptionName);
        await topicSender.SendMessageAsync(new ServiceBusMessage(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(topicUser))));

        using var store = GetDocumentStore();
        var sinkDone = WaitForQueueSinkBatch(store, (n, statistics) => statistics.ConsumeSuccesses >= 2);
        var config = SetupAzureServiceBusQueueSink(store, "this['@metadata']['@collection'] = 'Users'; put(this.Id, this)",
            new List<string>
            {
                AzureServiceBusSinkSource.Queue(UsersQueueName),
                AzureServiceBusSinkSource.Subscription(topicName, subscriptionName)
            });

        await AssertQueueSinkDoneAsync(sinkDone, TimeSpan.FromMinutes(1), store.Database, config);

        using var session = store.OpenSession();

        var fetchedQueueUser = session.Load<User>("users/1");
        Assert.NotNull(fetchedQueueUser);
        Assert.Equal("Queue", fetchedQueueUser.FirstName);

        var fetchedTopicUser = session.Load<User>("users/2");
        Assert.NotNull(fetchedTopicUser);
        Assert.Equal("Topic", fetchedTopicUser.FirstName);
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void Validation_AcceptsQueueAndSubscriptionEntries()
    {
        var config = BuildAzureServiceBusConfig("orders", AzureServiceBusSinkSource.Subscription("events", "billing"));

        Assert.True(config.Validate(out var errors));
        Assert.Empty(errors);
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void Validation_RejectsMalformedSubscriptionEntry()
    {
        var config = BuildAzureServiceBusConfig("topic;sub;extra");

        config.Validate(out var errors);

        Assert.NotEmpty(errors);
        Assert.Contains(errors, e => e.Contains("invalid"));
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void Validation_RejectsSubscriptionEntryWithEmptyHalf()
    {
        var config = BuildAzureServiceBusConfig("topic;");

        config.Validate(out var errors);

        Assert.NotEmpty(errors);
        Assert.Contains(errors, e => e.Contains("invalid"));
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void Validation_RejectsEmptyEntry()
    {
        var config = BuildAzureServiceBusConfig("");

        config.Validate(out var errors);

        Assert.NotEmpty(errors);
        Assert.Contains(errors, e => e.Contains("cannot be empty"));
    }

    private static QueueSinkConfiguration BuildAzureServiceBusConfig(params string[] entries)
    {
        var config = new QueueSinkConfiguration
        {
            Name = "test",
            ConnectionStringName = "test",
            BrokerType = QueueBrokerType.AzureServiceBus,
            Scripts =
            {
                new QueueSinkScript
                {
                    Name = "test",
                    Script = "put(this.Id, this)",
                    Queues = new List<string>(entries)
                }
            }
        };

        config.Initialize(new QueueConnectionString
        {
            Name = "Foo",
            BrokerType = QueueBrokerType.AzureServiceBus,
            AzureServiceBusConnectionSettings = new AzureServiceBusConnectionSettings
            {
                ConnectionString = "Endpoint=sb://example.servicebus.windows.net/;SharedAccessKeyName=fake;SharedAccessKey=ZmFrZQ=="
            }
        });

        return config;
    }

    [RavenFact(RavenTestCategory.Sinks, AzureServiceBusRequired = true)]
    public async Task ConnectionStringChanges_WillRestartAzureServiceBusSink()
    {
        var user1 = new User { Id = "users/1", FirstName = "John", LastName = "Doe" };
        byte[] userBytes1 = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(user1));

        var client = CreateAzureServiceBusClient();
        var sender = await CreateAzureServiceBusProducerAsync(client, UsersQueueName);
        await sender.SendMessageAsync(new ServiceBusMessage(userBytes1));

        using var store = GetDocumentStore();
        var config = SetupAzureServiceBusQueueSink(store, "this['@metadata']['@collection'] = 'Users'; put(this.Id, this)",
            new List<string> { UsersQueueName });

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var mre = new AsyncManualResetEvent();
        database.QueueSinkLoader.ProcessRemoved += _ => mre.Set();

        var sink = (AzureServiceBusQueueSink)database.QueueSinkLoader.Processes.SingleOrDefault();
        Assert.NotNull(sink);

        var originalConnectionString = GetConnectionString();

        store.Maintenance.Send(new PutConnectionStringOperation<QueueConnectionString>(new QueueConnectionString
        {
            Name = config.ConnectionStringName,
            BrokerType = QueueBrokerType.AzureServiceBus,
            AzureServiceBusConnectionSettings = new AzureServiceBusConnectionSettings
            {
                // append an extra trailing semicolon to make the connection string differ but still valid
                ConnectionString = originalConnectionString + ";"
            }
        }));

        Assert.True(await mre.WaitAsync(TimeSpan.FromSeconds(30)));

        var sink2 = (AzureServiceBusQueueSink)database.QueueSinkLoader.Processes.SingleOrDefault();

        Assert.NotNull(sink2);
        Assert.NotEqual(sink, sink2);
        Assert.False(sink.Configuration.Connection.IsEqual(sink2.Configuration.Connection));
    }

    [RavenFact(RavenTestCategory.Sinks, AzureServiceBusRequired = true)]
    public async Task FallbackOnConsumeErrors()
    {
        using var store = GetDocumentStore();

        var client = CreateAzureServiceBusClient();
        var sender = await CreateAzureServiceBusProducerAsync(client, UsersQueueName);

        var config = SetupAzureServiceBusQueueSink(store, "put(this.Id, this)", new List<string> { UsersQueueName });

        var database = await GetDatabase(store.Database);

        AzureServiceBusQueueSink sink = null;
        var readyDeadline = Stopwatch.StartNew();
        while (readyDeadline.Elapsed < TimeSpan.FromSeconds(15))
        {
            sink = database.QueueSinkLoader.Processes.OfType<AzureServiceBusQueueSink>().FirstOrDefault();
            if (sink != null)
                break;
            await Task.Delay(100);
        }

        Assert.NotNull(sink);

        sink.ForTestingPurposesOnly().BeforeConsume = () =>
            throw new InvalidOperationException("forced test failure");

        await sender.SendMessageAsync(new ServiceBusMessage("{}"));

        await WaitForGreaterThanAsync(() => Task.FromResult(sink.Statistics.ConsumeErrors), 2);

        Assert.True(sink.Statistics.ConsumeErrors >= 2,
            $"Expected at least 2 consume errors, got {sink.Statistics.ConsumeErrors}.");

        var alert = GetAlerts(database, config).Single();
        Assert.Contains(alert.Errors, x => x.Error.Contains("forced test failure"));

        var errorTimes = alert.Errors.Select(x => x.Date);
        var diffBetweenErrors = errorTimes.Zip(errorTimes.Skip(1), (first, second) => second - first);
        Assert.InRange(diffBetweenErrors.Min(), TimeSpan.FromSeconds(4), TimeSpan.FromSeconds(6));
    }
}
