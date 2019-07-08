using System;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Tests.Infrastructure.ConnectionString
{
    public class MongodbConnectionString
    {
        private static MongodbConnectionString _instance;
        public static MongodbConnectionString Instance => _instance ?? (_instance = new MongodbConnectionString());

        private Lazy<string> ConnectionString { get;}
        
        private MongodbConnectionString()
        {
            ConnectionString = new Lazy<string>(() =>
            {
                var connectionString = Environment.GetEnvironmentVariable("RAVEN_MONGODB_CONNECTION_STRING");
                return string.IsNullOrEmpty(connectionString) 
                    ? string.Empty
                    : connectionString;
            });
        }
        
        public bool CanConnect()
        {
            try
            {
                var client = new MongoClient(ConnectionString.Value);
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
