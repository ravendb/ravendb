using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Client.Subscriptions
{
    public class RegexSubscriptionTests : RavenTestBase
    {
        public RegexSubscriptionTests(ITestOutputHelper output) : base(output)
        {
        }

        private readonly TimeSpan _waitForDocTimeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(15);

        private static SubscriptionWorkerOptions CreateSubscriptionWorkerOptions(string subscriptionName)
        {
            return new SubscriptionWorkerOptions(subscriptionName)
            {
                MaxDocsPerBatch = 128,
                MaxErroneousPeriod = TimeSpan.FromHours(1),
                TimeToWaitBeforeConnectionRetry = TimeSpan.FromMinutes(10),
                CloseWhenNoDocsLeft = true,
                Strategy = SubscriptionOpeningStrategy.TakeOver,
            };
        }

        [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task SubscriptionWithRegexIsMatch_ShouldFilterCorrectly(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new RegexMe { Text = "i love dogs and cats" });
                    session.Store(new RegexMe { Text = "i love cats" });
                    session.Store(new RegexMe { Text = "i love dogs" });
                    session.Store(new RegexMe { Text = "i love bats" });
                    session.Store(new RegexMe { Text = "dogs love me" });
                    session.Store(new RegexMe { Text = "cats love me" });
                    session.SaveChanges();
                }

                const string pattern = "^[a-z ]{2,4}love";

                var subscriptionName = await store.Subscriptions.CreateAsync<RegexMe>(
                    x => Regex.IsMatch(x.Text, pattern));

                var workerOptions = CreateSubscriptionWorkerOptions(subscriptionName);

                using (var worker = store.Subscriptions.GetSubscriptionWorker<RegexMe>(workerOptions))
                {
                    var docs = new BlockingCollection<RegexMe>();

                    _ = worker.Run(batch => batch.Items.ForEach(i => docs.Add(i.Result)));

                    for (int i = 0; i < 4; i++)
                    {
                        Assert.True(docs.TryTake(out var doc, _waitForDocTimeout));
                        Assert.True(Regex.IsMatch(doc.Text, pattern));
                    }

                    Assert.False(docs.TryTake(out _));
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task SubscriptionWithRegexIsMatch_AndIgnoreCase_ShouldFilterCorrectly(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new RegexMe { Text = "I LOVE DOGS AND CATS" });
                    session.Store(new RegexMe { Text = "I LOVE CATS" });
                    session.Store(new RegexMe { Text = "I LOVE DOGS" });
                    session.Store(new RegexMe { Text = "I LOVE BATS" });
                    session.Store(new RegexMe { Text = "DOGS LOVE ME" });
                    session.Store(new RegexMe { Text = "CATS LOVE ME" });
                    session.SaveChanges();
                }

                const string pattern = "^[a-z ]{2,4}love";

                var subscriptionName = await store.Subscriptions.CreateAsync<RegexMe>(
                    x => Regex.IsMatch(x.Text, pattern, RegexOptions.IgnoreCase));

                var workerOptions = CreateSubscriptionWorkerOptions(subscriptionName);

                using (var worker = store.Subscriptions.GetSubscriptionWorker<RegexMe>(workerOptions))
                {
                    var docs = new BlockingCollection<RegexMe>();

                    _ = worker.Run(batch => batch.Items.ForEach(i => docs.Add(i.Result)));

                    for (int i = 0; i < 4; i++)
                    {
                        Assert.True(docs.TryTake(out var doc, _waitForDocTimeout));
                        Assert.True(Regex.IsMatch(doc.Text, pattern, RegexOptions.IgnoreCase));
                    }

                    Assert.False(docs.TryTake(out _));
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task SubscriptionWithWildcardRegexIsMatch_ShouldFilterCorrectly(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var pattern = "String or binary data would be truncated in table '*.dbo.SomeTable', column 'SomeColumn'. Truncated value: '*'. The statement has been terminated.";
                var wildcardPattern = WildcardToRegex(pattern);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(
                        new Message
                        {
                            Id = "240638ce-be95-4fbd-89ad-1f143f2a427a",
                            UserMessage = "String or binary data would be truncated in table 'SpecificCustomerDatabase.dbo.SomeTable', column 'SomeColumn'. Truncated value: 'blablabla'. The statement has been terminated."
                        }
                    );

                    await session.StoreAsync(
                        new Message
                        {
                            Id = "250638ce-be95-4fbd-89ad-1f143f2a428a",
                            UserMessage = "String or binary data would be truncated in table 'OtherCustomerDatabase.dbo.SomeTable', column 'SomeColumn'. Truncated value: 'zozozozo'. The statement has been terminated."
                        }
                    );

                    await session.StoreAsync(
                        new Message
                        {
                            Id = "340638ce-be95-4fbd-89ad-1f143f2a427b",
                            UserMessage = "Other message."
                        }
                    );

                    await session.SaveChangesAsync();
                }

                var subscriptionName = await store.Subscriptions.CreateAsync<Message>(
                    x => Regex.IsMatch(x.UserMessage, wildcardPattern));

                var workerOptions = CreateSubscriptionWorkerOptions(subscriptionName);

                using (var worker = store.Subscriptions.GetSubscriptionWorker<Message>(workerOptions))
                {
                    var docs = new BlockingCollection<Message>();

                    _ = worker.Run(batch => batch.Items.ForEach(i => docs.Add(i.Result)));

                    for (int i = 0; i < 2; i++)
                    {
                        Assert.True(docs.TryTake(out var doc, _waitForDocTimeout));
                        Assert.True(Regex.IsMatch(doc.UserMessage, wildcardPattern));
                    }

                    Assert.False(docs.TryTake(out _));
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task SubscriptionWithRegexIsMatch_AndSingleline_ShouldFilterCorrectly(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new RegexMe { Text = "first line\nsecond line" });
                    session.Store(new RegexMe { Text = "no newline here" });
                    session.SaveChanges();
                }

                const string pattern = "^first line.*second line$";

                var subscriptionName = await store.Subscriptions.CreateAsync<RegexMe>(
                    x => Regex.IsMatch(x.Text, pattern, RegexOptions.Singleline));

                var workerOptions = CreateSubscriptionWorkerOptions(subscriptionName);

                using (var worker = store.Subscriptions.GetSubscriptionWorker<RegexMe>(workerOptions))
                {
                    var docs = new BlockingCollection<RegexMe>();

                    _ = worker.Run(batch => batch.Items.ForEach(i => docs.Add(i.Result)));

                    Assert.True(docs.TryTake(out var doc, _waitForDocTimeout));
                    Assert.True(Regex.IsMatch(doc.Text, pattern, RegexOptions.Singleline));
                    Assert.False(Regex.IsMatch(doc.Text, pattern));

                    Assert.False(docs.TryTake(out _));
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task SubscriptionWithRegexIsMatch_AndMultiline_ShouldFilterCorrectly(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new RegexMe { Text = "line1\nmatch here" });
                    session.Store(new RegexMe { Text = "no match" });
                    session.SaveChanges();
                }

                const string pattern = "^match";

                var subscriptionName = await store.Subscriptions.CreateAsync<RegexMe>(
                    x => Regex.IsMatch(x.Text, pattern, RegexOptions.Multiline));

                var workerOptions = CreateSubscriptionWorkerOptions(subscriptionName);

                using (var worker = store.Subscriptions.GetSubscriptionWorker<RegexMe>(workerOptions))
                {
                    var docs = new BlockingCollection<RegexMe>();

                    _ = worker.Run(batch => batch.Items.ForEach(i => docs.Add(i.Result)));

                    Assert.True(docs.TryTake(out var doc, _waitForDocTimeout));
                    Assert.True(Regex.IsMatch(doc.Text, pattern, RegexOptions.Multiline));
                    Assert.False(Regex.IsMatch(doc.Text, pattern));

                    Assert.False(docs.TryTake(out _));
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public async Task SubscriptionWithRegexWithNotSupportedInput_ShouldFail()
        {
            using (var store = GetDocumentStore())
            {
                const string pattern = "^[a-z ]{2,4}love";

                await Assert.ThrowsAsync<NotSupportedException>(async () =>
                {
                    await store.Subscriptions.CreateAsync<Message>(x => Regex.IsMatch(x.UserMessage, pattern, RegexOptions.IgnoreCase, TimeSpan.FromSeconds(1)));
                });

                await Assert.ThrowsAsync<NotSupportedException>(async () =>
                {
                    await store.Subscriptions.CreateAsync<Message>(x => Regex.IsMatch(x.UserMessage, pattern, RegexOptions.ExplicitCapture));
                });

                await Assert.ThrowsAsync<NotSupportedException>(async () =>
                {
                    await store.Subscriptions.CreateAsync<Message>(x => Regex.IsMatch(x.UserMessage, pattern, RegexOptions.Compiled));
                });

                await Assert.ThrowsAsync<NotSupportedException>(async () =>
                {
                    await store.Subscriptions.CreateAsync<Message>(x => Regex.IsMatch(x.UserMessage, pattern, RegexOptions.Compiled));
                });

                await Assert.ThrowsAsync<NotSupportedException>(async () =>
                {
                    await store.Subscriptions.CreateAsync<Message>(x => Regex.IsMatch(x.UserMessage, pattern, RegexOptions.IgnorePatternWhitespace));
                });

                await Assert.ThrowsAsync<NotSupportedException>(async () =>
                {
                    await store.Subscriptions.CreateAsync<Message>(x => Regex.IsMatch(x.UserMessage, pattern, RegexOptions.RightToLeft));
                });

                await Assert.ThrowsAsync<NotSupportedException>(async () =>
                {
                    await store.Subscriptions.CreateAsync<Message>(x => Regex.IsMatch(x.UserMessage, pattern, RegexOptions.ECMAScript));
                });

                await Assert.ThrowsAsync<NotSupportedException>(async () =>
                {
                    await store.Subscriptions.CreateAsync<Message>(x => Regex.IsMatch(x.UserMessage, pattern, RegexOptions.CultureInvariant));
                });

                await Assert.ThrowsAsync<NotSupportedException>(async () =>
                {
                    await store.Subscriptions.CreateAsync<Message>(x => Regex.IsMatch(x.UserMessage, pattern, RegexOptions.NonBacktracking));
                });

                await Assert.ThrowsAsync<NotSupportedException>(async () =>
                {
                    await store.Subscriptions.CreateAsync<Message>(x => Regex.Match(x.UserMessage, pattern).Success);
                });
            }
        }

        private static string WildcardToRegex([StringSyntax(StringSyntaxAttribute.Regex)] string pattern)
        {
            var escaped = Regex.Escape(pattern);
            var replaced = escaped.
                Replace("\\*", ".*").
                Replace("\\?", ".");

            return "^" + replaced + "$";
        }

        private class RegexMe
        {
            public string Text { get; set; }
        }

        private class Message
        {
            public string Id { get; set; }

            public string UserMessage { get; set; }
        }
    }
}
