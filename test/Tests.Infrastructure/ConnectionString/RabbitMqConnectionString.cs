using System;
using RabbitMQ.Client;

namespace Tests.Infrastructure.ConnectionString;

public class RabbitMqConnectionString
{
    private const string EnvironmentVariable = "RAVEN_RABBITMQ_CONNECTION_STRING";

    private static RabbitMqConnectionString _instance;

    public static RabbitMqConnectionString Instance => _instance ??= new RabbitMqConnectionString();

    private RabbitMqConnectionString()
    {
        VerifiedConnectionString = new Lazy<string>(VerifiedNodesValueFactory);

        ConnectionString = new Lazy<string>(() => Environment.GetEnvironmentVariable(EnvironmentVariable) ?? string.Empty);
    }

    private Lazy<string> ConnectionString { get; }

    public Lazy<string> VerifiedConnectionString { get; }

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
        var localConnectionString = "amqp://guest:guest@localhost:5672/";

        if (TryConnect(localConnectionString, out _))
            return localConnectionString;

        if (ConnectionString.Value.Length == 0)
            throw new InvalidOperationException($"Environment variable {EnvironmentVariable} is empty");


        if (TryConnect(ConnectionString.Value, out var ex))
            return ConnectionString.Value;

        throw new InvalidOperationException($"Can't access Kafka instance. Provided url: {ConnectionString.Value}", ex);


        bool TryConnect(string connectionString, out Exception exception)
        {
            try
            {
                var connectionFactory = new ConnectionFactory() {Uri = new Uri(connectionString)};

                using (connectionFactory.CreateConnection())
                {

                }

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
