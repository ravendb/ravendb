using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public class SlowSqlDetails : INotificationDetails
    {
        public const int MaxNumberOfStatements = 500;

        public Queue<SlowSqlStatementInfo> Statements { get; set; }

        public SlowSqlDetails()
        {
            Statements = new Queue<SlowSqlStatementInfo>();
        }

        public void Add(SlowSqlStatementInfo slowSql)
        {
            Statements.Enqueue(slowSql);

            if (Statements.Count > MaxNumberOfStatements)
                Statements.TryDequeue(out _);
        }

        public DynamicJsonValue ToJson()
        {
            var result = new DynamicJsonValue();

            var statements = new DynamicJsonArray();

            foreach (var details in Statements)
            {
                statements.Add(details.ToJson());
            }

            result[nameof(Statements)] = statements;

            return result;
        }
    }
}
