using System;
using System.ComponentModel.Composition.Hosting;
using System.Linq;

using Raven.Bundles.UniqueConstraints;
using Raven.Client.Embedded;
using Raven.Client.UniqueConstraints;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bundles.UniqueConstraints.Bugs
{
    public class MultiTenancy : RavenTest
    {
        [Fact]
        public void Round_Trip_Includes_Expected_Metadata()
        {
            var original = new User { Email = "foo@bar.com", Username = "Foo" };

            using (var documentStore = InitializeDocumentStore(new UniqueConstraintsStoreListener(new CustomUniqueConstraintsTypeDictionary())))
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(original);
                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var roundTripped = session.LoadByUniqueConstraint<User>(u => u.Email, original.Email);
                    var metadata = session.Advanced.GetMetadataFor(roundTripped);
                    var constraints = metadata.Value<RavenJArray>("Ensure-Unique-Constraints");
                
                    Assert.Equal(2, constraints.Length);
                }
            }
        }

        [Fact]
        public void Constraints_Are_Unique_Per_Tenant()
        {
            var original = new User { Email = "foo@bar.com", Username = "Foo" };

            using (var documentStore1 = InitializeDocumentStore(new UniqueConstraintsStoreListener(new CustomUniqueConstraintsTypeDictionary()), 8078))
            using (var documentStore2 = InitializeDocumentStore(new UniqueConstraintsStoreListener()))
            {
                using (var session1 = documentStore1.OpenSession())
                using (var session2 = documentStore2.OpenSession())
                {
                    session1.Store(original);
                    session2.Store(original);

                    session1.SaveChanges();
                    session2.SaveChanges();
                }

                using (var session1 = documentStore1.OpenSession())
                using (var session2 = documentStore2.OpenSession())
                {
                    var roundTripped1 = session1.LoadByUniqueConstraint<User>(u => u.Username, original.Username);
                    var metadata1 = session1.Advanced.GetMetadataFor(roundTripped1);
                    var constraints1 = metadata1.Value<RavenJArray>("Ensure-Unique-Constraints");

                    var roundTripped2 = session2.LoadByUniqueConstraint<User>(u => u.Username, original.Username);
                    var metadata2 = session2.Advanced.GetMetadataFor(roundTripped2);
                    var constraints2 = metadata2.Value<RavenJArray>("Ensure-Unique-Constraints");

                    Assert.NotEqual(constraints1.Count(), constraints2.Count());
                }
            }
        }

        private EmbeddableDocumentStore InitializeDocumentStore(UniqueConstraintsStoreListener listener, int port = 8079)
        {
            var documentStore = NewDocumentStore(port: port, configureStore: store =>
            {
                store.Configuration.Catalog.Catalogs.Add(new AssemblyCatalog(typeof(UniqueConstraintsPutTrigger).Assembly));
                store.RegisterListener(listener);
            },activeBundles: "Unique Constraints");

            return documentStore;
        }

        class User
        {
            public string Id { get; set; }

            // Email is optionally unique based on tenant configuration
            public string Email { get; set; }

            [UniqueConstraint(CaseInsensitive=true)]
            public string Username { get; set; }
        }

        class CustomUniqueConstraintsTypeDictionary : UniqueConstraintsTypeDictionary
        {
            protected override ConstraintInfo[] GetUniqueProperties(Type type)
            {
                var props = base.GetUniqueProperties(type);

                if (type == typeof(User))
                {
                    var ci = new ReflectedConstraintInfo(typeof(User).GetProperty("Email"), null);
                    ci.Configuration.CaseInsensitive = true;

                    props = props.Union(new ConstraintInfo[] { ci }).ToArray();
                }

                return props;
            }
        }
    }
}
