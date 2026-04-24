using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client.Queries
{
    public class OnlyOfTypeTests(ITestOutputHelper output) : RavenTestBase(output)
    {
        
        /// <summary>
        /// Options assigning both, <see cref="Animal"/> and <see cref="Dog"/> under one
        /// </summary>
        private static readonly Options CollectionAwareOptions = new()
        {
            ModifyDocumentStore = store =>
                store.Conventions.FindCollectionName = type =>
                    typeof(Animal).IsAssignableFrom(type)
                        ? "Animals"
                        : DocumentConventions.DefaultGetCollectionName(type)
        };

        private class Animal
        {
            public string Name { get; set; }
        }

        private class Dog : Animal
        {
            public string Breed { get; set; }
        }

        private class AnimalView
        {
            public string Name { get; set; }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.ClientApi)]
        public void RqlGeneration_LinqForm()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenSession())
            {
                var query = session.Query<Animal>().OnlyOfType<Dog>();
                Assert.Equal("from 'Animals' where @metadata.Raven-Clr-Type = $p0", query.ToString());
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.ClientApi)]
        public void RqlGeneration_FluentSyncForm()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenSession())
            {
                var query = session.Advanced.DocumentQuery<Animal>().OnlyOfType<Dog>();
                Assert.Equal("from 'Animals' where @metadata.Raven-Clr-Type = $p0", query.ToString());
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.ClientApi)]
        public async Task RqlGeneration_FluentAsyncForm()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenAsyncSession())
            {
                var query = session.Advanced.AsyncDocumentQuery<Animal>().OnlyOfType<Dog>();
                Assert.Equal("from 'Animals' where @metadata.Raven-Clr-Type = $p0", query.ToString());
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.ClientApi)]
        public void RqlGeneration_ParameterValueMatchesConvention()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenSession())
            {
                var query = session.Advanced.DocumentQuery<Animal>().OnlyOfType<Dog>();
                var iq = query.GetIndexQuery();
                Assert.Equal("from 'Animals' where @metadata.Raven-Clr-Type = $p0", iq.Query);
                var expectedClrTypeName = store.Conventions.GetClrTypeName(typeof(Dog));
                Assert.Equal(expectedClrTypeName, iq.QueryParameters["p0"]);
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.ClientApi)]
        public void EndToEnd_FiltersByClrType()
        {
            using (var store = GetDocumentStore(CollectionAwareOptions))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Animal { Name = "Generic" });
                    session.Store(new Dog { Name = "Rex", Breed = "Labrador" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var dogs = session.Query<Animal>()
                        .OnlyOfType<Dog>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.Equal(1, dogs.Count);
                    Assert.Equal("Rex", dogs[0].Name);
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.ClientApi)]
        public void EndToEnd_NarrowsElementType()
        {
            using (var store = GetDocumentStore(CollectionAwareOptions))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Animal { Name = "Generic" });
                    session.Store(new Dog { Name = "Rex", Breed = "Labrador" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var dogs = session.Query<Animal>()
                        .OnlyOfType<Dog>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.Equal(1, dogs.Count);
                    Assert.Equal("Labrador", dogs[0].Breed);
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.ClientApi)]
        public void EndToEnd_ComposedWithSelect()
        {
            using (var store = GetDocumentStore(CollectionAwareOptions))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Animal { Name = "Generic" });
                    session.Store(new Dog { Name = "Rex", Breed = "Labrador" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var views = session.Query<Animal>()
                        .OnlyOfType<Dog>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Select(x => new AnimalView { Name = x.Name })
                        .ToList();

                    Assert.Equal(1, views.Count);
                    Assert.Equal("Rex", views[0].Name);
                }
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.ClientApi)]
        public void EndToEnd_ConventionOverride_UsesCustomClrTypeName()
        {
            const string customTypeName = "my-custom-dog-type";

            var options = new Options
            {
                ModifyDocumentStore = store =>
                {
                    store.Conventions.FindCollectionName = type =>
                        typeof(Animal).IsAssignableFrom(type)
                            ? "Animals"
                            : DocumentConventions.DefaultGetCollectionName(type);
                    store.Conventions.FindClrTypeName = type =>
                        type == typeof(Dog)
                            ? customTypeName
                            : DocumentConventions.DefaultGetCollectionName(type);
                }
            };

            using (var store = GetDocumentStore(options))
            using (var session = store.OpenSession())
            {
                var query = session.Advanced.DocumentQuery<Animal>().OnlyOfType<Dog>();
                var iq = query.GetIndexQuery();
                Assert.Equal(customTypeName, iq.QueryParameters["p0"]);
            }
        }
    }
}
