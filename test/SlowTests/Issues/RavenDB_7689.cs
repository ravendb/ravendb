using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Client.Attachments;
using Microsoft.AspNetCore.Hosting.Internal;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.Server.Operations.ApiKeys;
using Raven.Client.Util;
using Raven.Server.Config.Attributes;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Server.Documents.Notifications;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_7689 : ClusterTestBase
    {
        [Fact]
        public async Task InvalidateTheAccessTokenCache()
        {
            var apiKey = new ApiKeyDefinition
            {
                Enabled = true,
                Secret = "secret",
            };

            var leader = await CreateRaftClusterAndGetLeader(3);
            using (var store = GetDocumentStore(defaultServer: leader, replicationFactor: 3, apiKey: "super/" + apiKey.Secret))
            {
                var requestExecutor = store.GetRequestExecutor();

                Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.Admin;

                apiKey.ResourcesAccessMode[store.Database] = AccessModes.ReadOnly;
                store.Admin.Server.Send(new PutApiKeyOperation("super", apiKey));

                Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.None;

                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var document = context.ReadObject(new DynamicJsonValue
                    {
                        ["Name"] = "Fitzchak"
                    }, "users/1");
                    var command = new PutDocumentCommand("users/2", null, document, context);
                    foreach (var node in requestExecutor.TopologyNodes)
                    {
                        await Assert.ThrowsAsync<AuthorizationException>(async () => await requestExecutor.ExecuteAsync(node, context, command));
                    }
                }
                

                Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.Admin;

                apiKey.ResourcesAccessMode[store.Database] = AccessModes.ReadWrite;
                store.Admin.Server.Send(new PutApiKeyOperation("super", apiKey));

                Server.Configuration.Server.AnonymousUserAccessMode = AnonymousUserAccessModeValues.None;


                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var document = context.ReadObject(new DynamicJsonValue
                    {
                        ["Name"] = "Fitzchak"
                    }, "users/1");
                    var command = new PutDocumentCommand("users/2", null, document, context);
                    foreach (var node in requestExecutor.TopologyNodes)
                    {
                        await requestExecutor.ExecuteAsync(node, context, command);
                    }
                }
            }
        }
    }
}
