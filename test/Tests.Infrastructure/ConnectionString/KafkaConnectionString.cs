using System;
using Confluent.Kafka;

namespace Tests.Infrastructure.ConnectionString;

public class KafkaConnectionString
{
    private const string EnvironmentVariable = "RAVEN_KAFKA_URL";

    private static KafkaConnectionString _instance;

    public static KafkaConnectionString Instance => _instance ??= new KafkaConnectionString();

    private KafkaConnectionString()
    {
        VerifiedUrl = new Lazy<string>(VerifiedNodesValueFactory);

        Url = new Lazy<string>(() => Environment.GetEnvironmentVariable(EnvironmentVariable) ?? string.Empty);
    }

    private Lazy<string> Url { get; }

    public Lazy<string> VerifiedUrl { get; }

    public bool CanConnect()
    {
        try
        {
            VerifiedNodesValueFactory();
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    protected virtual string VerifiedNodesValueFactory()
    {
        var singleLocalNode = "localhost:29092";

        if (TryConnect(singleLocalNode, out _))
            return singleLocalNode;

        if (Url.Value.Length == 0)
            throw new InvalidOperationException($"Environment variable {EnvironmentVariable} is empty");


        if (TryConnect(Url.Value, out var ex))
            return Url.Value;

        throw new InvalidOperationException($"Can't access Kafka instance. Provided url: {Url.Value}", ex);


        bool TryConnect(string url, out Exception exception)
        {
            try
            {
                var config = new AdminClientConfig() { BootstrapServers = url };
                var adminClient = new AdminClientBuilder(config).Build(); 
                
                adminClient.GetMetadata(TimeSpan.FromSeconds(10));
                exception = null;

                return true;
            }
            catch (Exception e)
            {
                exception = e;
                return false;
            }
        }
    }
}
