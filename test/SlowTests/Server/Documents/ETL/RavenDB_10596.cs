using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json.Parsing;
using Sparrow.Server.Collections;
using Xunit;

namespace SlowTests.Server.Documents.ETL
{
    public class RavenDB_10596 : EtlTestBase
    {
        [Fact]
        public async Task AggregatesTransformationErrorsInSingleAlert()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script: @"throw 'super exception';
                                       loadToUsers(this);");

                var database = await GetDatabase(src.Database);

                var notifications = new AsyncQueue<DynamicJsonValue>();
                using (database.NotificationCenter.TrackActions(notifications, null))
                {
                    for (int i = 0; i < 3; i++)
                    {
                        using (var session = src.OpenSession())
                        {
                            session.Store(new User()
                            {
                                Name = "Joe Doe"
                            }, $"users/{i}");

                            session.SaveChanges();
                        }

                        // let's ignore notification that's triggered by document put (DatabaseStatsChanged)

                        DynamicJsonValue alert = null;

                        for (int attempt = 0; attempt < 2; attempt++)
                        {
                            var notification = await notifications.TryDequeueAsync(TimeSpan.FromSeconds(30));

                            Assert.True(notification.Item1);

                            if (notification.Item2[nameof(Notification.Type)].ToString() == NotificationType.AlertRaised.ToString())
                                alert = notification.Item2;
                        }

                        Assert.NotNull(alert);

                        var details = (DynamicJsonValue)alert[nameof(AlertRaised.Details)];
                        var errors = (DynamicJsonArray)details[nameof(EtlErrorsDetails.Errors)];

                        Assert.Equal(i + 1, errors.Items.Count);
                        
                        for (int j = 0; j < i + 1; j++)
                        {
                            var error = (DynamicJsonValue)errors.Items.ToArray()[j];

                            Assert.Equal($"users/{j}", error[nameof(EtlErrorInfo.DocumentId)]);
                            Assert.Contains("super exception", error[nameof(EtlErrorInfo.Error)].ToString());
                            Assert.NotNull(error[nameof(EtlErrorInfo.Date)]);
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task CanAddAndUpdateLoadErrors()
        {
            using (var store = GetDocumentStore())
            {
                var database = await GetDatabase(store.Database);

                var transformationErrors = new Queue<EtlErrorInfo>();

                transformationErrors.Enqueue(new EtlErrorInfo
                {
                    DocumentId = "items/1",
                    Date = DateTime.UtcNow,
                    Error = "fatal load error"
                });

                var notifications = new AsyncQueue<DynamicJsonValue>();
                using (database.NotificationCenter.TrackActions(notifications, null))
                {
                    database.NotificationCenter.EtlNotifications.AddLoadErrors("Raven ETL Test", "test", transformationErrors);

                    var alert = await GetAlert(notifications);

                    Assert.Equal(NotificationType.AlertRaised.ToString(), alert[nameof(Notification.Type)]);
                    Assert.Equal("Raven ETL Test: 'test'", alert[nameof(AlertRaised.Title)]);
                    Assert.Contains("Loading transformed data to the destination has failed", alert[nameof(AlertRaised.Message)].ToString());

                    var details = (DynamicJsonValue)alert[nameof(AlertRaised.Details)];
                    var errors = (DynamicJsonArray)details[nameof(EtlErrorsDetails.Errors)];

                    Assert.Equal(1, errors.Items.Count);

                    var error = (DynamicJsonValue)errors.Items.First();

                    Assert.Equal("items/1", error[nameof(EtlErrorInfo.DocumentId)]);
                    Assert.Equal("fatal load error", error[nameof(EtlErrorInfo.Error)].ToString());
                    Assert.NotNull(error[nameof(EtlErrorInfo.Date)]);

                    // add error for items/1 (should update) and for items/2 (new error)

                    transformationErrors.Enqueue(new EtlErrorInfo
                    {
                        DocumentId = "items/2",
                        Date = DateTime.UtcNow,
                        Error = "fatal load error2"
                    });

                    database.NotificationCenter.EtlNotifications.AddLoadErrors("Raven ETL Test", "test", transformationErrors);

                    alert = await GetAlert(notifications);

                    details = (DynamicJsonValue)alert[nameof(AlertRaised.Details)];
                    errors = (DynamicJsonArray)details[nameof(EtlErrorsDetails.Errors)];

                    Assert.Equal(2, errors.Items.Count);

                    error = (DynamicJsonValue)errors.Items.Last();

                    Assert.Equal("items/2", error[nameof(EtlErrorInfo.DocumentId)]);
                    Assert.Equal("fatal load error2", error[nameof(EtlErrorInfo.Error)].ToString());
                    Assert.NotNull(error[nameof(EtlErrorInfo.Date)]);
                    
                    // add a lot of errors - should not be more than 500

                    for (int i = 0; i < EtlErrorsDetails.MaxNumberOfErrors + 1; i++)
                    {
                        transformationErrors.Enqueue(new EtlErrorInfo
                        {
                            DocumentId = $"items/{i}",
                            Date = DateTime.UtcNow,
                            Error = "fatal load error"
                        });
                    }

                    database.NotificationCenter.EtlNotifications.AddLoadErrors("Raven ETL Test", "test", transformationErrors);

                    alert = await GetAlert(notifications);

                    details = (DynamicJsonValue)alert[nameof(AlertRaised.Details)];
                    errors = (DynamicJsonArray)details[nameof(EtlErrorsDetails.Errors)];

                    Assert.Equal(EtlErrorsDetails.MaxNumberOfErrors, errors.Items.Count);
                }
            }
        }

        [Fact]
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
