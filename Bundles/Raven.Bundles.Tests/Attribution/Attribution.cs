//-----------------------------------------------------------------------
// <copyright file="Attribution.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
extern alias database;
using System;
using System.ComponentModel.Composition.Hosting;
using System.IO;
using System.Reflection;
using Raven.Bundles.Attribution.Data;
using Raven.Bundles.Attribution.Triggers;
using Raven.Client.Attribution;
using Raven.Client.Document;
using Raven.Server;
using Xunit;

namespace Raven.Bundles.Tests.Attribution
{
    public class Attribution : IDisposable
    {
        private readonly DocumentStore documentStore;
        private readonly string path;
        private readonly RavenDbServer ravenDbServer;

        public Attribution()
        {
            path = Path.GetDirectoryName(Assembly.GetAssembly(typeof(Attribution)).CodeBase);
            path = Path.Combine(path, "TestDb").Substring(6);
            database::Raven.Database.Extensions.IOExtensions.DeleteDirectory(path);
            ravenDbServer = new RavenDbServer(
                new database::Raven.Database.Config.RavenConfiguration
                {
                    Port = 58080,
                    DataDirectory = path,
                    RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
                    Catalog =
                    {
                        Catalogs =
                        {
                            new AssemblyCatalog(typeof(AttributionPutTrigger).Assembly)
                        }
                    },
                });
            documentStore = new DocumentStore
            {
                Url = "http://localhost:58080"
            };
            documentStore.Initialize();
            using (var s = documentStore.OpenSession())
            {
                s.Store(new AttributionConfiguration
                {
                    Exclude = true,
                    Id = "Raven/Attribution/Users",
                });
                s.Store(new AttributionConfiguration
                {
                    Exclude = true,
                    Id = "Raven/Attribution/Comments",
                });
                s.Store(new AttributionConfiguration
                {
                    Exclude = false,
                    Id = "Raven/Attribution/DefaultConfiguration",
                });
                s.SaveChanges();
            }
        }

        #region IDisposable Members

        public void Dispose()
        {
            documentStore.Dispose();
            ravenDbServer.Dispose();
            database::Raven.Database.Extensions.IOExtensions.DeleteDirectory(path);
        }

        #endregion

        [Fact]
        public void Will_automatically_set_metadata()
        {
            var company = new Company { Name = "Company Name" };
            using (var session = documentStore.OpenSession())
            {
                session.AttributeTo("users/1");
                session.Store(company);
                session.SaveChanges();
            }

            using (var session = documentStore.OpenSession())
            {
                var company2 = session.Load<Company>(company.Id);
                var metadata = session.Advanced.GetMetadataFor(company2);
                Assert.Equal("users/1", metadata.Value<string>("Raven-Document-Author"));
            }
        }

        [Fact]
        public void Can_exclude_entities_from_attribution()
        {
            var user = new User { Name = "User Name" };
            var comment = new Comment { Name = "foo" };
            using (var session = documentStore.OpenSession())
            {
                session.AttributeTo("users/1");
                session.Store(user);
                session.Store(comment);
                session.SaveChanges();
            }

            using (var sesion = documentStore.OpenSession())
            {
                var metadata = sesion.Advanced.GetMetadataFor(sesion.Load<User>(user.Id));
                Assert.Null(metadata.Value<string>("Raven-Document-Author"));

                metadata = sesion.Advanced.GetMetadataFor(sesion.Load<Comment>(comment.Id));
                Assert.Null(metadata.Value<string>("Raven-Document-Author"));
            }
        }

        [Fact]
        public void Will_automatically_overwrite_metadata_on_next_insert()
        {
            var company = new Company { Name = "Company Name" };
            using (var session = documentStore.OpenSession())
            {
                session.AttributeTo("users/1");
                session.Store(company);
                session.SaveChanges();
                session.AttributeTo("users/2");
                company.Name = "Hibernating Rhinos";
                session.SaveChanges();
            }

            using (var session = documentStore.OpenSession())
            {
                var company2 = session.Load<Company>(company.Id);
                var metadata = session.Advanced.GetMetadataFor(company2);
                Assert.Equal("users/2", metadata.Value<string>("Raven-Document-Author"));
            }
        }

        #region Nested type: Comment

        public class Comment
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        #endregion

        #region Nested type: User

        public class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        #endregion
    }

    public class Company
    {
        public string Name { get; set; }

        public string Id { get; set; }
    }
}
