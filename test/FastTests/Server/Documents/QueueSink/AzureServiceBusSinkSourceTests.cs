using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.QueueSink;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents.QueueSink;

public class AzureServiceBusSinkSourceTests : RavenTestBase
{
    public AzureServiceBusSinkSourceTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void Queue_ReturnsQueueName_WhenNameValid()
    {
        Assert.Equal("my-queue", AzureServiceBusSinkSource.Queue("my-queue"));
    }

    [RavenTheory(RavenTestCategory.Sinks)]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Queue_Throws_WhenNameEmpty(string name)
    {
        Assert.Throws<ArgumentException>(() => AzureServiceBusSinkSource.Queue(name));
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void Queue_Throws_WhenNameContainsSeparator()
    {
        Assert.Throws<ArgumentException>(() => AzureServiceBusSinkSource.Queue("foo;bar"));
    }

    [RavenFact(RavenTestCategory.Sinks)]
    public void Subscription_EncodesTopicAndSubscription()
    {
        Assert.Equal("topic;sub", AzureServiceBusSinkSource.Subscription("topic", "sub"));
    }

    [RavenTheory(RavenTestCategory.Sinks)]
    [InlineData(null, "sub")]
    [InlineData("", "sub")]
    [InlineData("   ", "sub")]
    [InlineData("topic", null)]
    [InlineData("topic", "")]
    [InlineData("topic", "   ")]
    public void Subscription_Throws_WhenArgumentEmpty(string topic, string subscription)
    {
        Assert.Throws<ArgumentException>(() => AzureServiceBusSinkSource.Subscription(topic, subscription));
    }

    [RavenTheory(RavenTestCategory.Sinks)]
    [InlineData("to;pic", "sub")]
    [InlineData("topic", "su;b")]
    public void Subscription_Throws_WhenArgumentContainsSeparator(string topic, string subscription)
    {
        Assert.Throws<ArgumentException>(() => AzureServiceBusSinkSource.Subscription(topic, subscription));
    }

    [RavenTheory(RavenTestCategory.Sinks)]
    [InlineData("my-queue")]
    [InlineData("topic;sub")]
    public void Configuration_Validates_WhenEntriesValid(string entry)
    {
        var config = BuildConfiguration(entry);
        Assert.True(config.Validate(out var errors, validateName: true, validateConnection: false),
            "Expected configuration to be valid. Errors: " + string.Join(", ", errors));
    }

    [RavenTheory(RavenTestCategory.Sinks)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(";sub")]
    [InlineData("topic;")]
    [InlineData(";")]
    [InlineData("topic;sub;extra")]
    public void Configuration_RejectsInvalidEntries(string entry)
    {
        var config = BuildConfiguration(entry);
        Assert.False(config.Validate(out var errors, validateName: true, validateConnection: false),
            "Expected validation failure for entry: " + entry);
        Assert.NotEmpty(errors);
    }

    private static QueueSinkConfiguration BuildConfiguration(string entry)
    {
        return new QueueSinkConfiguration
        {
            Name = "sink",
            BrokerType = QueueBrokerType.AzureServiceBus,
            ConnectionStringName = "cs",
            Scripts =
            {
                new QueueSinkScript
                {
                    Name = "script",
                    Script = "put(this.Id, this)",
                    Queues = new List<string> { entry }
                }
            }
        };
    }

    [RavenTheory(RavenTestCategory.Sinks)]
    [InlineData("Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=key;SharedAccessKey=abc")]
    [InlineData("endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=key;SharedAccessKey=abc")]
    [InlineData("SharedAccessKeyName=key;Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKey=abc")]
    [InlineData("Endpoint=sb://ns.servicebus.windows.net")]
    public void AzureServiceBusConnectionSettings_ValidConnectionString(string connectionString)
    {
        var settings = new AzureServiceBusConnectionSettings { ConnectionString = connectionString };
        Assert.True(settings.IsValidConnection(), $"Expected valid: {connectionString}");
    }

    // Client-side IsValidConnection is intentionally shallow — it just confirms the string
    // contains "sb://". Deeper validation is deferred to the Azure SDK on the server so that
    // Raven.Client doesn't need to depend on Azure.Messaging.ServiceBus.
    [RavenTheory(RavenTestCategory.Sinks)]
    [InlineData("SharedAccessKeyName=key;SharedAccessKey=abc")] // no sb://
    [InlineData("Endpoint=https://ns.servicebus.windows.net/;SharedAccessKey=abc")] // wrong scheme
    [InlineData("nothing-useful-here")]
    public void AzureServiceBusConnectionSettings_InvalidConnectionString(string connectionString)
    {
        var settings = new AzureServiceBusConnectionSettings { ConnectionString = connectionString };
        Assert.False(settings.IsValidConnection(), $"Expected invalid: {connectionString}");
    }
}
