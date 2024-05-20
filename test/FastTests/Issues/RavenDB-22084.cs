using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_22084 : RavenTestBase
    {
        public RavenDB_22084(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void StoreOnDictionaryWithEnumKeyShouldWork()
        {
            using var store = GetDocumentStore();

            string id;
            using (var session = store.OpenSession())
            {
                var entity = Machine.Create();
                session.Store(entity);
                session.SaveChanges();
                id = session.Advanced.GetDocumentId(entity);
            }

            var storedJson = GetRawJson(store, id);
            Assert.Equal("""
                {
                  "Checks": {
                    "Engine": "Good",
                    "Gears": "Good"
                  }
                }
                """, storedJson);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void StoreOnDictionaryWithEnumKeyAndSaveEnumsAsIntegersShouldWork()
        {
            using var store = GetDocumentStore(new()
            {
                ModifyDocumentStore = store =>
                {
                    store.Conventions.SaveEnumsAsIntegers = true;
                },
            });

            string id;
            using (var session = store.OpenSession())
            {
                var entity = Machine.Create();
                session.Store(entity);
                session.SaveChanges();
                id = session.Advanced.GetDocumentId(entity);
            }

            var storedJson = GetRawJson(store, id);
            Assert.Equal("""
                {
                  "Checks": {
                    "Engine": 1,
                    "Gears": 1
                  }
                }
                """, storedJson);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void PatchOnDictionaryWithEnumKeyShouldWork()
        {
            using var store = GetDocumentStore();

            string id;
            using (var session = store.OpenSession())
            {
                var entity = Machine.Create();
                session.Store(entity);
                session.SaveChanges();
                id = session.Advanced.GetDocumentId(entity);
            }

            using (var session = store.OpenSession())
            {
                session.Advanced.Patch<Machine, Status>(id, x => x.Checks[Parts.Engine], Status.Bad);
                session.SaveChanges();
            }

            var patchedJson = GetRawJson(store, id);
            Assert.Equal("""
                {
                  "Checks": {
                    "Engine": "Bad",
                    "Gears": "Good"
                  }
                }
                """, patchedJson);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void PatchOnDictionaryWithEnumKeyAndSaveEnumsAsIntegersShouldWork()
        {
            using var store = GetDocumentStore(new()
            {
                ModifyDocumentStore = store => store.Conventions.SaveEnumsAsIntegers = true,
            });

            string id;
            using (var session = store.OpenSession())
            {
                var entity = Machine.Create();
                session.Store(entity);
                session.SaveChanges();
                id = session.Advanced.GetDocumentId(entity);
            }

            using (var session = store.OpenSession())
            {
                session.Advanced.Patch<Machine, Status>(id, x => x.Checks[Parts.Engine], Status.Bad);
                session.SaveChanges();
            }

            var patchedJson = GetRawJson(store, id);
            Assert.Equal("""
                {
                  "Checks": {
                    "Engine": 2,
                    "Gears": 1
                  }
                }
                """, patchedJson);
        }

        private string GetRawJson(DocumentStore store, string id)
        {
            using var session = store.OpenSession();
            using var stream = new MemoryStream();
            session.Advanced.LoadIntoStream([id], stream);
            stream.Position = 0;
            var response = JObject.Load(new Newtonsoft.Json.JsonTextReader(new StreamReader(stream)));
            var results = response.GetValue("Results") as JArray;
            var single = results.Single() as JObject;
            single.Remove("@metadata");
            return single.ToString();
        }

        private class Machine
        {
            public static Machine Create() => new()
            {
                Checks = new()
                {
                    { Parts.Engine, Status.Good },
                    { Parts.Gears, Status.Good },
                },
            };

            public Dictionary<Parts, Status> Checks { get; set; }
        }

        private enum Parts
        {
            Engine = 42,
            Gears = 102,
        }

        private enum Status
        {
            None,
            Good,
            Bad,
        }
    }
}
