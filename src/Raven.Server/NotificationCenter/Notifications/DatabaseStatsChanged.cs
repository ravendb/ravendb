using System.Collections.Generic;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications
{
    public class DatabaseStatsChanged : Notification
    {
        private DatabaseStatsChanged() : base(NotificationType.DatabaseStatsChanged)
        {
        }

        public override string Id { get; }

        public long CountOfDocuments { get; private set; }

        public long CountOfIndexes { get; private set; }

        public long CountOfStaleIndexes { get; private set; }

        public List<DocumentsStorage.CollectionStats> ModifiedCollections { get; private set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(CountOfDocuments)] = CountOfDocuments;
            json[nameof(CountOfIndexes)] = CountOfIndexes;
            json[nameof(CountOfStaleIndexes)] = CountOfStaleIndexes;
            json[nameof(ModifiedCollections)] = ModifiedCollections;

            return json;
        }

        public static DatabaseStatsChanged Create(long countOfDocs, int countOfIndexes, int countOfStaleIndexes, List<DocumentsStorage.CollectionStats> modifiedCollections)
        {
            return new DatabaseStatsChanged
            {
                IsPersistent = false,
                Title = null,
                Message = null,
                Severity = NotificationSeverity.Info,
                CountOfDocuments = countOfDocs,
                CountOfIndexes = countOfIndexes,
                CountOfStaleIndexes = countOfStaleIndexes,
                ModifiedCollections = modifiedCollections
            };
        }
    }
}