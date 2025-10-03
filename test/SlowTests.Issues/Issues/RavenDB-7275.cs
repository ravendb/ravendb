namespace SlowTests.Issues
{
    /*public class RavenDB_7275 : RavenTestBase
    {
        private readonly ApiKeyDefinition _apiKey = new ApiKeyDefinition
        {
            Enabled = true,
            Secret = "secret",
            ResourcesAccessMode =
            {
                  ["db/CanGetDocWithValidToken"] = AccessMode.ReadWrite,
                ["db/CanGetTokenFromServer"] = AccessMode.Admin
            }
        };


        [RavenFact(RavenTestCategory.Subscriptions)]
       public async Task ValidateSubscriptionAuthorizationRejectOnCreationAsync()
        {
            DoNotReuseServer();
             Server.Configuration.Security.AuthenticationEnabled = false;
             AccessMode[] modes = { AccessMode.None, AccessMode.ReadOnly };
            using (var store = GetDocumentStore(apiKey: "super/" + _apiKey.Secret))
            {
                foreach (var accessMode in modes)
                {
                    Server.Configuration.Security.AuthenticationEnabled = false;
                    _apiKey.ResourcesAccessMode[store.Database] = accessMode;

                    store.Admin.Server.Send(new PutCertificateOperation("super", _apiKey));
                    var doc = store.Admin.Server.Send(new GetCertificateOperation("super"));
                    Assert.NotNull(doc);

                    Server.Configuration.Security.AuthenticationEnabled = true;

                    await Assert.ThrowsAsync<AuthorizationException>(async () => await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions<User>()));
                }
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task ValidateSubscriptionAuthorizationAcceptOnCreation()
        {
            DoNotReuseServer();
            Server.Configuration.Security.AuthenticationEnabled = false;
            AccessMode[] modes = { AccessMode.ReadWrite, AccessMode.Admin };
            using (var store = GetDocumentStore(apiKey: "super/" + _apiKey.Secret))
            {
                foreach (var accessMode in modes)
                {
                    Server.Configuration.Security.AuthenticationEnabled = false;
                    _apiKey.ResourcesAccessMode[store.Database] = accessMode;

                    store.Admin.Server.Send(new PutCertificateOperation("super", _apiKey));
                    var doc = store.Admin.Server.Send(new GetCertificateOperation("super"));
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

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task ValidateSubscriptionAuthorizationRejectOnOpening()
        {
            DoNotReuseServer();
           Server.Configuration.Security.AuthenticationEnabled = false;
            AccessMode[] modes = {AccessMode.None, AccessMode.ReadOnly};
            foreach (var accessMode in modes)
            {
                using (var store = GetDocumentStore(apiKey: "super/" + _apiKey.Secret))
                {
                    Server.Configuration.Security.AuthenticationEnabled = false;
                    _apiKey.ResourcesAccessMode[store.Database] = accessMode;

                    var subscriptionId = await store.Subscriptions.CreateAsync(
                        new SubscriptionCreationOptions<User>());

                    store.Admin.Server.Send(new PutCertificateOperation("super", _apiKey));
                    var doc = store.Admin.Server.Send(new GetCertificateOperation("super"));
                    Assert.NotNull(doc);

                     Server.Configuration.Security.AuthenticationEnabled = true;
                    var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId)
                    {
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(200)
                    });
                    await Assert.ThrowsAsync<AuthorizationException>(async () => await subscription.Run(user => { }));
                }
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task ValidateSubscriptionAuthorizationAcceptOnOpening()
        {
            DoNotReuseServer();
            Server.Configuration.Security.AuthenticationEnabled = false;
             AccessMode[] modes = { AccessMode.ReadWrite, AccessMode.Admin };
            using (var store = GetDocumentStore(apiKey: "super/" + _apiKey.Secret))
            {
                foreach (var accessMode in modes)
                {
                    Server.Configuration.Security.AuthenticationEnabled = false;
                    _apiKey.ResourcesAccessMode[store.Database] = accessMode;

                    var subscriptionId = await store.Subscriptions.CreateAsync(
                        new SubscriptionCreationOptions<User>());

                    store.Admin.Server.Send(new PutCertificateOperation("super", _apiKey));
                    var doc = store.Admin.Server.Send(new GetCertificateOperation("super"));
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
    }*/
}
