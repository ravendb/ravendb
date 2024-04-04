using System;
using System.Runtime.Loader;
using RabbitMQ.Client;

namespace Tests.Infrastructure.ConnectionString;

public class RabbitMqConnectionString : IDisposable
{
    private const string EnvironmentVariable = "RAVEN_RABBITMQ_CONNECTION_STRING";

    private static RabbitMqConnectionString _instance;

    public static RabbitMqConnectionString Instance => _instance ??= new RabbitMqConnectionString();
    
    private IConnection _connection;

    private readonly Lazy<string> _initialized;

    private RabbitMqConnectionString()
    {
        _initialized = new Lazy<string>(EnsureInitialize);
    }

    private string EnsureInitialize()
    {
        var localConnectionString = "amqp://guest:guest@localhost:5672/";

        _connection = CreateConnection(localConnectionString, out _);
        if (_connection != null)
            return localConnectionString;

        var envConnectionString = Environment.GetEnvironmentVariable(EnvironmentVariable) ?? string.Empty;
        if (envConnectionString.Length == 0)
            throw new InvalidOperationException($"Environment variable {EnvironmentVariable} is empty");

        _connection = CreateConnection(envConnectionString, out var ex);
        if (_connection != null)
            return envConnectionString;

        throw new InvalidOperationException($"Can't create connection for Kafka instance. Provided url: {envConnectionString}", ex);

        IConnection CreateConnection(string connectionString, out Exception exception)
        {
            exception = null;
            try
            {
                var connectionFactory = new ConnectionFactory() {Uri = new Uri(connectionString)};
                var conn = connectionFactory.CreateConnection();
                
                // connection succeeded register for disposable
                AssemblyLoadContext.Default.Unloading += _ =>
                {
                    try
                    {
                        Dispose();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }
                };
                return conn;

            }
            catch (Exception e)
            {
                exception = e;
                return null;
            }
        }
    }

    public IModel CreateModel()
    {
        _ = _initialized.Value;
        return _connection.CreateModel();
    }

    public string VerifiedConnectionString => _initialized.Value;

    public bool CanConnect => CanConnectInternal();

    private bool CanConnectInternal()
    {
        try
        {
            _ = _initialized.Value;
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    public void Dispose()
    {
        using (_connection)
        {
            _connection?.Close();
        }
    }
}
