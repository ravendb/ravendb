using System;
using System.Threading.Tasks;
using FastTests;
using FastTests.Client.Attachments;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.Server.Operations.ApiKeys;
using Raven.Client.Util;
using Raven.Server.Config.Attributes;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Server.Documents.Notifications;
using Sparrow;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_7275 : RavenTestBase
    {
        private readonly ApiKeyDefinition _apiKey = new ApiKeyDefinition
        {
            Enabled = true,
            Secret = "secret",
            ResourcesAccessMode =
            {
                ["db/CanGetDocWithValidToken"] = AccessModes.ReadWrite,
                ["db/CanGetTokenFromServer"] = AccessModes.Admin
            }
        };


        [Fact]
        public void ValidateSubscriptionAuthorizationRejectOnCreation()
        {
            DoNotReuseServer();
            Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.Admin;
            AccessModes[] modes = { AccessModes.None, AccessModes.ReadOnly };
            using (var store = GetDocumentStore(apiKey: "super/" + _apiKey.Secret))
            {
                foreach (var accessMode in modes)
                {
                    Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.Admin;
                    _apiKey.ResourcesAccessMode[store.Database] = accessMode;

                    store.Admin.Server.Send(new PutApiKeyOperation("super", _apiKey));
                    var doc = store.Admin.Server.Send(new GetApiKeyOperation("super"));
                    Assert.NotNull(doc);

                    Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.None;

                    Assert.Throws<AuthorizationException>(() => AsyncHelpers.RunSync(() => store.AsyncSubscriptions.CreateAsync(
                        new SubscriptionCreationOptions<User>())));
                }
            }
        }

        [Fact]
        public async Task ValidateSubscriptionAuthorizationAcceptOnCreation()
        {
            DoNotReuseServer();
            Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.Admin;
            AccessModes[] modes = { AccessModes.ReadWrite, AccessModes.Admin };
            using (var store = GetDocumentStore(apiKey: "super/" + _apiKey.Secret))
            {
                foreach (var accessMode in modes)
                {
                    Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.Admin;
                    _apiKey.ResourcesAccessMode[store.Database] = accessMode;

                    store.Admin.Server.Send(new PutApiKeyOperation("super", _apiKey));
                    var doc = store.Admin.Server.Send(new GetApiKeyOperation("super"));
                    Assert.NotNull(doc);

                    Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.None;

                    var subscriptionId = await store.AsyncSubscriptions.CreateAsync(new SubscriptionCreationOptions<User>());

                    var subscription = store.AsyncSubscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId)
                    {
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(200)
                    });

                    var mre = new AsyncManualResetEvent();
                    subscription.Subscribe(x => { });

                    subscription.AfterAcknowledgment += mre.Set;

                    await subscription.StartAsync();

                    await mre.WaitAsync(TimeSpan.FromSeconds(20));
                }
            }
        }

        [Fact]
        public async Task ValidateSubscriptionAuthorizationRejectOnOpening()
        {
            DoNotReuseServer();
            Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.Admin;
            AccessModes[] modes = { AccessModes.None, AccessModes.ReadOnly };
            using (var store = GetDocumentStore(apiKey: "super/" + _apiKey.Secret))
            {
                foreach (var accessMode in modes)
                {
                    Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.Admin;
                    _apiKey.ResourcesAccessMode[store.Database] = accessMode;

                    var subscriptionId = await store.AsyncSubscriptions.CreateAsync(
                        new SubscriptionCreationOptions<User>());

                    store.Admin.Server.Send(new PutApiKeyOperation("super", _apiKey));
                    var doc = store.Admin.Server.Send(new GetApiKeyOperation("super"));
                    Assert.NotNull(doc);

                    Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.None;

                    var subscription = store.AsyncSubscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId)
                    {
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(200)
                    });
                    subscription.Subscribe(x => { });
                    Assert.Throws<AuthorizationException>(() => AsyncHelpers.RunSync(() => subscription.StartAsync()));
                }
            }
        }

        [Fact]
        public async Task ValidateSubscriptionAuthorizationAcceptOnOpening()
        {
            DoNotReuseServer();
            Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.Admin;
            AccessModes[] modes = { AccessModes.ReadWrite, AccessModes.Admin };
            using (var store = GetDocumentStore(apiKey: "super/" + _apiKey.Secret))
            {
                foreach (var accessMode in modes)
                {
                    Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.Admin;
                    _apiKey.ResourcesAccessMode[store.Database] = accessMode;

                    var subscriptionId = await store.AsyncSubscriptions.CreateAsync(
                        new SubscriptionCreationOptions<User>());

                    store.Admin.Server.Send(new PutApiKeyOperation("super", _apiKey));
                    var doc = store.Admin.Server.Send(new GetApiKeyOperation("super"));
                    Assert.NotNull(doc);

                    Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.None;

                    var subscription = store.AsyncSubscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId)
                    {
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(200)
                    });

                    var mre = new AsyncManualResetEvent();
                    subscription.Subscribe(x => { });

                    subscription.AfterAcknowledgment += mre.Set;

                    await subscription.StartAsync();

                    await mre.WaitAsync(TimeSpan.FromSeconds(20));
                }
            }
        }
    }
}
