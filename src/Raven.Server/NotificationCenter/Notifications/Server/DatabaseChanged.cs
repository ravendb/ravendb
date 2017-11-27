using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Server
{
    public class DatabaseChanged : Notification
    {
        private DatabaseChanged(string database) : base(NotificationType.DatabaseChanged, database)
        {
            DatabaseName = database;
        }

        public override string Id => $"{Type}/{ChangeType}/{DatabaseName}";

        public string DatabaseName { get; private set; }

        public DatabaseChangeType ChangeType { get; private set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(DatabaseName)] = DatabaseName;
            json[nameof(ChangeType)] = ChangeType;

            return json;
        }

        public static DatabaseChanged Create(string databaseName, DatabaseChangeType change)
        {
            return new DatabaseChanged(databaseName)
            {
                ChangeType = change
            };
        }
    }
}
