using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Tests.Core.Utils.Entities;
using Raven.Server.Documents.ETL;
using Sparrow.Json.Parsing;
using Sparrow.Server.Collections;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.ETL
{
    public class RavenDB_10596 : RavenTestBase
    {
        public RavenDB_10596(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task AggregatesTransformationErrorsInSingleAlert()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                var etlDone = Etl.WaitForEtlToComplete(src);
                
                Etl.AddEtl(src, dest, "Users", script: @"throw 'super exception';
                                       loadToUsers(this);");

                var database = await GetDatabase(src.Database);

                for (int i = 0; i < 3; i++)
                {
                    etlDone.Reset();

                    using (var session = src.OpenSession())
                    {
                        session.Store(new User()
                            {
                                Name = "Joe Doe"
                            }, $"users/{i}");

                        session.SaveChanges();
                    }

                    await etlDone.WaitAsync(TimeSpan.FromSeconds(5));

                    var itemErrors = database.TaskErrorsStorage.ReadAllItemErrors(TaskCategory.Etl);

                    Assert.Equal(i + 1, itemErrors.Count);

                    Assert.Equal($"users/{i}", itemErrors[i].DocumentId);
                    Assert.Contains("super exception", itemErrors[i].Error);
                    Assert.NotNull(itemErrors[i].CreatedAt);
                }
            }
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task CanAddAndSlowSqlWarnings()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                var slowStatements = new Queue<SlowSqlStatementInfo>();

                slowStatements.Enqueue(new SlowSqlStatementInfo
                {
                    Date = DateTime.UtcNow,
                    Statement = "insert",
                    Duration = 1
                });

                var notifications = new AsyncQueue<DynamicJsonValue>();
                using (database.NotificationCenter.TrackActions(notifications, null))
                {
                    database.NotificationCenter.EtlNotifications.AddSlowSqlWarnings("Raven ETL Test", "test", slowStatements);

                    var hint = await GetAlert(notifications);

                    Assert.Equal(NotificationType.PerformanceHint.ToString(), hint[nameof(Notification.Type)]);
                    Assert.Equal("Raven ETL Test: 'test'", hint[nameof(PerformanceHint.Title)]);
                    Assert.Contains("Slow SQL detected", hint[nameof(PerformanceHint.Message)].ToString());

                    var details = (DynamicJsonValue)hint[nameof(PerformanceHint.Details)];
                    var statements = (DynamicJsonArray)details[nameof(SlowSqlDetails.Statements)];

                    Assert.Equal(1, statements.Items.Count);

                    var statementInfo = (DynamicJsonValue)statements.Items.First();

                    Assert.Equal("insert", statementInfo[nameof(SlowSqlStatementInfo.Statement)]);
                    Assert.Equal(1L, statementInfo[nameof(SlowSqlStatementInfo.Duration)]);
                    Assert.NotNull(statementInfo[nameof(SlowSqlStatementInfo.Date)]);

                    slowStatements.Enqueue(new SlowSqlStatementInfo
                    {
                        Date = DateTime.UtcNow,
                        Statement = "insert",
                        Duration = 1
                    });

                    database.NotificationCenter.EtlNotifications.AddSlowSqlWarnings("Raven ETL Test", "test", slowStatements);

                    hint = await GetAlert(notifications);

                    details = (DynamicJsonValue)hint[nameof(AlertRaised.Details)];
                    statements = (DynamicJsonArray)details[nameof(SlowSqlDetails.Statements)];

                    Assert.Equal(3, statements.Items.Count);

                    statementInfo = (DynamicJsonValue)statements.Items.Last();

                    Assert.Equal("insert", statementInfo[nameof(SlowSqlStatementInfo.Statement)]);
                    Assert.Equal(1L, statementInfo[nameof(SlowSqlStatementInfo.Duration)]);
                    Assert.NotNull(statementInfo[nameof(SlowSqlStatementInfo.Date)]);
                    
                    // add a lot of errors - should not be more than 500

                    for (int i = 0; i < SlowSqlDetails.MaxNumberOfStatements + 1; i++)
                    {
                        slowStatements.Enqueue(new SlowSqlStatementInfo
                        {
                            Date = DateTime.UtcNow,
                            Statement = "insert",
                            Duration = 1
                        });
                    }

                    database.NotificationCenter.EtlNotifications.AddSlowSqlWarnings("Raven ETL Test", "test", slowStatements);

                    hint = await GetAlert(notifications);

                    details = (DynamicJsonValue)hint[nameof(AlertRaised.Details)];
                    statements = (DynamicJsonArray)details[nameof(SlowSqlDetails.Statements)];

                    Assert.Equal(SlowSqlDetails.MaxNumberOfStatements, statements.Items.Count);
                }
            }
        }

        private static async Task<DynamicJsonValue> GetAlert(AsyncQueue<DynamicJsonValue> notifications)
        {
            var notification = await notifications.TryDequeueAsync(TimeSpan.FromSeconds(30));

            Assert.True(notification.Item1);

            return notification.Item2;
        }
    }
}
