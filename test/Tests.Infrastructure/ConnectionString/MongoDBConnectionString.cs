using System;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Tests.Infrastructure.ConnectionString
{
    public class MongoDBConnectionString
    {
        private static MongoDBConnectionString _instance;
        public static MongoDBConnectionString Instance => _instance ??= new MongoDBConnectionString();

        public Lazy<string> ConnectionString { get; }

        private readonly Lazy<bool> _canConnect;

        public bool CanConnect => _canConnect.Value;

        private MongoDBConnectionString()
        {
            ConnectionString = new Lazy<string>(() =>
            {
                var connectionString = Environment.GetEnvironmentVariable("RAVEN_MONGODB_CONNECTION_STRING");
                return string.IsNullOrEmpty(connectionString)
                    ? string.Empty
                    : connectionString;
            });

            _canConnect = new Lazy<bool>(CanConnectInternal);
        }

        private bool CanConnectInternal()
        {
            try
            {
                var connectionString = ConnectionString.Value;
                if (string.IsNullOrEmpty(connectionString))
                    return false;

                var client = new MongoClient(connectionString);
                var adminDb = client.GetDatabase("admin");
                var isMongoLive = adminDb.RunCommandAsync((Command<BsonDocument>)"{ping:1}").Wait(1000);

                return isMongoLive;
            }
            catch
            {
                return false;
            }
        }
    }
}
