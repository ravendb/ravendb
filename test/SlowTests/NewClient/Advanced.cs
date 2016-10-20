using System;
using System.Threading.Tasks;

using FastTests;
using Microsoft.AspNetCore.Hosting.Internal;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Json.Linq;
using Sparrow.Json;
using Xunit;

using Company = SlowTests.Core.Utils.Entities.Company;
using User = SlowTests.Core.Utils.Entities.User;

namespace SlowTests.NewClient
{
    public class Advanced : RavenTestBase
    {
        [Fact]
        public void CanGetChangesInformation()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    Assert.False(session.Advanced.HasChanges);

                    var user = new User { Id = "users/1", Name = "John" };
                    session.Store(user);

                    Assert.True(session.Advanced.HasChanged(user));
                    var x = session.Advanced.HasChanges;
                    Assert.True(session.Advanced.HasChanges);

                    session.SaveChanges();

                    Assert.False(session.Advanced.HasChanged(user));
                    Assert.False(session.Advanced.HasChanges);

                    user.AddressId = "addresses/1";
                    Assert.True(session.Advanced.HasChanged(user));
                    Assert.True(session.Advanced.HasChanges);

                    var whatChanged = session.Advanced.WhatChanged();
                    Assert.Equal("AddressId", ((DocumentsChanges[])whatChanged["users/1"])[0].FieldName);
                    Assert.Equal(null, ((DocumentsChanges[])whatChanged["users/1"])[0].FieldOldValue);
                    Assert.Equal("addresses/1", ((DocumentsChanges[])whatChanged["users/1"])[0].FieldNewValue.ToString());

                    session.Advanced.Clear();
                    Assert.False(session.Advanced.HasChanges);

                    var user2 = new User { Id = "users/2", Name = "John" };
                    session.Store(user2);
                    session.Delete(user2);

                    Assert.True(session.Advanced.HasChanged(user2));
                    Assert.True(session.Advanced.HasChanges);
                }
            }
        }

        [Fact]
        public void CanUseEvict()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    var user = new User { Id = "users/1", Name = "John" };

                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
                {
                    Assert.Equal(0, session.Advanced.NumberOfRequests);

                    session.Load<User>("users/1");

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var user = session.Load<User>("users/1");

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    session.Advanced.Evict(user);

                    session.Load<User>("users/1");

                    Assert.Equal(2, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void CanUseClear()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Id = "users/1", Name = "John" };

                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
                {
                    Assert.Equal(0, session.Advanced.NumberOfRequests);

                    session.Load<User>("users/1");

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    session.Load<User>("users/1");

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    session.Advanced.Clear();

                    session.Load<User>("users/1");

                    Assert.Equal(2, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void CanUseIsLoaded()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    var user = new User { Id = "users/1", Name = "John" };

                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
                {
                    Assert.False(session.Advanced.IsLoaded("users/1"));

                    session.Load<User>("users/1");

                    Assert.True(session.Advanced.IsLoaded("users/1"));
                    Assert.False(session.Advanced.IsLoaded("users/2"));

                    session.Advanced.Clear();

                    Assert.False(session.Advanced.IsLoaded("users/1"));
                }
            }
        }

        [Fact]
        public void CanUseRefresh()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    var user = new User { Id = "users/1", Name = "John" };

                    session.Store(user);
                    session.SaveChanges();
                }

                using (var session = store.OpenNewSession())
                {
                    var user = session.Load<User>("users/1");

                    Assert.NotNull(user);
                    Assert.Equal("John", user.Name);

                    //TODO - Change when we have new DatabaseCommands.Get and Put
                    long etag;
                    InMemoryDocumentSessionOperations.DocumentInfo newMetadata;
                    var document = TempGetCommand(session, out etag, out newMetadata);

                    newMetadata.Entity = session.ConvertToEntity(typeof(User), "users/1", document);
                    ((User)newMetadata.Entity).Name = "Jonathan";
                    TempPutCommand(session, newMetadata, etag);

                    user = session.Load<User>("users/1");

                    Assert.NotNull(user);
                    Assert.Equal("John", user.Name);

                    session.Advanced.Refresh(user);

                    Assert.NotNull(user);
                    Assert.Equal("Jonathan", user.Name);
                }
            }
        }

        private static void TempPutCommand(DocumentSession session, InMemoryDocumentSessionOperations.DocumentInfo newMetadata, long etag)
        {
            var newDocument = session.EntityToBlittable.ConvertEntityToBlittable(newMetadata.Entity, newMetadata);
            var putCommand = new PutDocumentCommand()
            {
                Id = "users/1",
                Etag = etag,
                Document = newDocument,
                Context = session.Context
            };
            session.RequestExecuter.Execute(putCommand, session.Context);
        }

        private static BlittableJsonReaderObject TempGetCommand(DocumentSession session, out long etag,
            out InMemoryDocumentSessionOperations.DocumentInfo newMetadata)
        {
            var command = new GetDocumentCommand
            {
                Ids = new[] {"users/1"}
            };
            session.RequestExecuter.Execute(command, session.Context);
            var document = (BlittableJsonReaderObject) command.Result.Results[0];
            BlittableJsonReaderObject metadata;
            if (document.TryGet(Constants.Metadata.Key, out metadata) == false)
                throw new InvalidOperationException("Document must have a metadata");
            string id;
            if (metadata.TryGet(Constants.Metadata.Id, out id) == false)
                throw new InvalidOperationException("Document must have an id");
            if (metadata.TryGet(Constants.Metadata.Etag, out etag) == false)
                throw new InvalidOperationException("Document must have an etag");
            newMetadata = new InMemoryDocumentSessionOperations.DocumentInfo
            {
                Id = id,
                Document = document,
                Metadata = metadata,
                ETag = etag
            };
            return document;
        }

        [Fact]
        public void CanUseOptmisticConcurrency()
        {
            const string entityId = "users/1";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    Assert.False(session.Advanced.UseOptimisticConcurrency);
                    session.Advanced.UseOptimisticConcurrency = true;

                    session.Store(new User { Id = entityId, Name = "User1" });
                    session.SaveChanges();

                    using (var otherSession = store.OpenNewSession())
                    {
                        var otherUser = otherSession.Load<User>(entityId);
                        otherUser.Name = "OtherName";
                        otherSession.Store(otherUser);
                        otherSession.SaveChanges();
                    }

                    var user = session.Load<User>("users/1");
                    user.Name = "Name";
                    session.Store(user);
                    var e = Assert.Throws<ConcurrencyException>(() => session.SaveChanges());
                    //Assert.Equal("PUT attempted on document '" + entityId + "' using a non current etag", e.Message);
                }
            }
        }

        [Fact]
        public void CanGetDocumentMetadata()
        {
            const string companyId = "companies/1";
            const string attrKey = "SetDocumentMetadataTestKey";
            const string attrVal = "SetDocumentMetadataTestValue";

            using (var store = GetDocumentStore())
            {
                store.DatabaseCommands.Put(
                    companyId,
                    null,
                    RavenJObject.FromObject(new Company { Id = companyId }),
                    new RavenJObject { { attrKey, attrVal } }
                    );

                using (var session = store.OpenNewSession())
                {
                    var company = session.Load<Company>(companyId);
                    var result = session.Advanced.GetMetadataFor<Company>(company);
                    Assert.NotNull(result);
                    //Assert.Equal(attrVal, result.Value<string>(attrKey));
                }
            }
        }

        [Fact]
        public void CanMarkReadOnly()
        {
            const string categoryName = "MarkReadOnlyTest";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewSession())
                {
                    session.Store(new Company { Id = "companies/1" });
                    session.SaveChanges();

                    var company = session.Load<Company>("companies/1");
                    session.Advanced.MarkReadOnly(company);
                    company.Name = categoryName;
                    Assert.True(session.Advanced.HasChanges);

                    session.Store(company);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    Assert.Equal("true", session.Advanced.GetMetadataFor<Company>(company)["Raven-Read-Only"]);
                }
            }
        }
    }
}
