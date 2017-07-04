using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.Server.Operations.ApiKeys;
using Raven.Client.Util;
using Raven.Tests.Core.Utils.Entities;
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
            Server.Configuration.Security.AuthenticationEnabled = false;
            AccessModes[] modes = { AccessModes.None, AccessModes.ReadOnly };
            using (var store = GetDocumentStore(apiKey: "super/" + _apiKey.Secret))
            {
                foreach (var accessMode in modes)
                {
                    Server.Configuration.Security.AuthenticationEnabled = false;
                    _apiKey.ResourcesAccessMode[store.Database] = accessMode;

                    store.Admin.Server.Send(new PutApiKeyOperation("super", _apiKey));
                    var doc = store.Admin.Server.Send(new GetApiKeyOperation("super"));
                    Assert.NotNull(doc);

                    Server.Configuration.Security.AuthenticationEnabled = true;

                    Assert.Throws<AuthorizationException>(() => AsyncHelpers.RunSync(() => store.Subscriptions.CreateAsync(
                        new SubscriptionCreationOptions<User>())));
                }
            }
        }

        [Fact]
        public async Task ValidateSubscriptionAuthorizationAcceptOnCreation()
        {
            DoNotReuseServer();
            Server.Configuration.Security.AuthenticationEnabled = false;
            AccessModes[] modes = { AccessModes.ReadWrite, AccessModes.Admin };
            using (var store = GetDocumentStore(apiKey: "super/" + _apiKey.Secret))
            {
                foreach (var accessMode in modes)
                {
                    Server.Configuration.Security.AuthenticationEnabled = false;
                    _apiKey.ResourcesAccessMode[store.Database] = accessMode;

                    store.Admin.Server.Send(new PutApiKeyOperation("super", _apiKey));
                    var doc = store.Admin.Server.Send(new GetApiKeyOperation("super"));
                    Assert.NotNull(doc);

                    Server.Configuration.Security.AuthenticationEnabled = true;

                    var subscriptionId = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions<User>());

                    var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId)
                    {
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(200)
                    });

                    var mre = new AsyncManualResetEvent();


                    subscription.AfterAcknowledgment += b => { mre.Set(); return Task.CompletedTask; };

                    GC.KeepAlive(subscription.Run(x => { }));

                    await mre.WaitAsync(TimeSpan.FromSeconds(20));
                }
            }
        }

        [Fact]
        public async Task ValidateSubscriptionAuthorizationRejectOnOpening()
        {
            DoNotReuseServer();
            Server.Configuration.Security.AuthenticationEnabled = false;
            AccessModes[] modes = { AccessModes.None, AccessModes.ReadOnly };
            using (var store = GetDocumentStore(apiKey: "super/" + _apiKey.Secret))
            {
                foreach (var accessMode in modes)
                {
                    Server.Configuration.Security.AuthenticationEnabled = false;
                    _apiKey.ResourcesAccessMode[store.Database] = accessMode;

                    var subscriptionId = await store.Subscriptions.CreateAsync(
                        new SubscriptionCreationOptions<User>());

                    store.Admin.Server.Send(new PutApiKeyOperation("super", _apiKey));
                    var doc = store.Admin.Server.Send(new GetApiKeyOperation("super"));
                    Assert.NotNull(doc);

                    Server.Configuration.Security.AuthenticationEnabled = true;

                    var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId)
                    {
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(200)
                    });

                    Assert.Throws<AuthorizationException>(() => AsyncHelpers.RunSync(() => subscription.Run(user => { })));
                }
            }
        }

        [Fact]
        public async Task ValidateSubscriptionAuthorizationAcceptOnOpening()
        {
            DoNotReuseServer();
            Server.Configuration.Security.AuthenticationEnabled = false;
            AccessModes[] modes = { AccessModes.ReadWrite, AccessModes.Admin };
            using (var store = GetDocumentStore(apiKey: "super/" + _apiKey.Secret))
            {
                foreach (var accessMode in modes)
                {
                    Server.Configuration.Security.AuthenticationEnabled = false;
                    _apiKey.ResourcesAccessMode[store.Database] = accessMode;

                    var subscriptionId = await store.Subscriptions.CreateAsync(
                        new SubscriptionCreationOptions<User>());

                    store.Admin.Server.Send(new PutApiKeyOperation("super", _apiKey));
                    var doc = store.Admin.Server.Send(new GetApiKeyOperation("super"));
                    Assert.NotNull(doc);

                    Server.Configuration.Security.AuthenticationEnabled = true;

                    var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId)
                    {
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(200)
                    });

                    var mre = new AsyncManualResetEvent();

                    subscription.AfterAcknowledgment += b => { mre.Set(); return Task.CompletedTask; };

                    GC.KeepAlive(subscription.Run(x => { }));

                    await mre.WaitAsync(TimeSpan.FromSeconds(20));
                }
            }
        }
    }
}
