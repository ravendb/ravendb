// -----------------------------------------------------------------------
//  <copyright file="DocumentReplication.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Json.Linq;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace Raven.Tests.Core.Replication
{
    public class DocumentReplication : RavenReplicationCoreTest
    {
#if DNXCORE50
        public DocumentReplication(TestServerFixture fixture)
            : base(fixture)
        {

        }
#endif

        [Fact]
        public async Task CanReplicateDocument()
        {
            using (var source = GetDocumentStore())
            using (var destination = GetDocumentStore())
            {
                SetupReplication(source, destinations: destination);

                using (var session = source.OpenAsyncSession())
                {
                    var user = new User {Name = "Arek"};
                    
                    await session.StoreAsync(user);

                    await session.SaveChangesAsync();
                    
                    await source.Replication.WaitAsync(etag: session.Advanced.GetEtagFor(user));
                }

                using (var session = destination.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");

                    Assert.NotNull(user);
                    Assert.Equal("Arek", user.Name);
                }
            }
        }

        [Fact]
        public void CanReplicateDocumentDeletion()
        {
            using (var source = GetDocumentStore())
            using (var destination = GetDocumentStore())
            {
                SetupReplication(source, destinations: destination);

                source.DatabaseCommands.Put("docs/1", null, new RavenJObject() {{"Key", "Value"}}, new RavenJObject());

                var document = WaitForDocument(destination, "docs/1");

                Assert.NotNull(document);

                source.DatabaseCommands.Delete("docs/1", null);

                for (int i = 0; i < RetriesCount; i++)
                {
                    if (destination.DatabaseCommands.Get("docs/1") == null)
                        break;
                    Thread.Sleep(100);
                }

                Assert.Null(destination.DatabaseCommands.Get("docs/1"));
            }
        }
    }
}
