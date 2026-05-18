using System;
using System.Runtime.Loader;
using Microsoft.IdentityModel.Protocols.Configuration;
using RabbitMQ.Client;

namespace Tests.Infrastructure.ConnectionString;

public class RabbitMqConnectionString : IDisposable
{
    private static RabbitMqConnectionString _instance;

    public static RabbitMqConnectionString Instance => _instance ??= new RabbitMqConnectionString();

    private IConnection _connection;

    private readonly Lazy<bool> _canConnect;

    private Lazy<string> Url { get; }

    public Lazy<string> VerifiedUrl { get; }

    private RabbitMqConnectionString()
    {
        VerifiedUrl = new Lazy<string>(VerifiedNodesValueFactory);

        Url = new Lazy<string>(() => RavenTestHelper.EnvironmentVariables.RabbitMqConnectionString ?? string.Empty);

        _canConnect = new Lazy<bool>(CanConnectInternal);
    }

    protected virtual string VerifiedNodesValueFactory()
    {
        var localConnectionString = "amqp://guest:guest@localhost:5672/";

        _connection = CreateConnection(localConnectionString, out _);
        if (_connection != null)
            return localConnectionString;

        if (Url.Value.Length == 0)
            throw new InvalidConfigurationException($"Environment variable {RavenTestHelper.EnvironmentVariables.RabbitMqConnectionStringKey} is empty");

        _connection = CreateConnection(Url.Value, out var ex);
        if (_connection != null)
            return Url.Value;

        throw new InvalidOperationException($"Can't create connection for Rabbit MQ instance. Provided url: {Url.Value}", ex);

        IConnection CreateConnection(string connectionString, out Exception exception)
        {
            exception = null;
            try
            {
                var connectionFactory = new ConnectionFactory() { Uri = new Uri(connectionString), RequestedConnectionTimeout = TimeSpan.FromSeconds(2) };
                var conn = connectionFactory.CreateConnectionAsync().GetAwaiter().GetResult();

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

    public IChannel CreateChannel()
    {
        _ = _canConnect.Value;
        return _connection.CreateChannelAsync().GetAwaiter().GetResult();
    }

    public bool CanConnect => CanConnectInternal();

    private bool CanConnectInternal()
    {
        try
        {
            var url = Url.Value;
            if (string.IsNullOrEmpty(url))
                return false;

            VerifiedNodesValueFactory();
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
            _connection?.CloseAsync().GetAwaiter().GetResult();
        }
    }
}
