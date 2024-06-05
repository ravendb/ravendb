using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents;
using Sparrow.Json;
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
            Assert.Equal("""{"Checks":{"Engine":"Good","Gears":"Good"}}""", storedJson);
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
            Assert.Equal("""{"Checks":{"Engine":1,"Gears":1}}""", storedJson);
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

            var storedJson = GetRawJson(store, id);
            Assert.Equal("""{"Checks":{"Engine":"Good","Gears":"Good"}}""", storedJson);

            using (var session = store.OpenSession())
            {
                session.Advanced.Patch<Machine, Status>(id, x => x.Checks[Parts.Engine], Status.Bad);
                session.SaveChanges();
            }

            var patchedJson = GetRawJson(store, id);
            Assert.Equal("""{"Checks":{"Engine":"Bad","Gears":"Good"}}""", patchedJson);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void PatchOnDictionaryWithEnumKeyAndSaveEnumsAsIntegersShouldBeInconsistent()
        {
            // https://github.com/ravendb/ravendb/pull/18530#discussion_r1612819842
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
            Assert.Equal("""{"Checks":{"Engine":1,"Gears":1}}""", storedJson);

            using (var session = store.OpenSession())
            {
                session.Advanced.Patch<Machine, Status>(id, x => x.Checks[Parts.Engine], Status.Bad);
                session.SaveChanges();
            }

            var patchedJson = GetRawJson(store, id);
            Assert.Equal("""{"Checks":{"Engine":"Bad","Gears":1}}""", patchedJson);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void PatchOnDictionaryWithEnumKeyAndSaveEnumsAsIntegersAndSaveEnumsAsIntegersForPatchingShouldWork()
        {
            using var store = GetDocumentStore(new()
            {
                ModifyDocumentStore = store =>
                {
                    store.Conventions.SaveEnumsAsIntegers = true;
                    store.Conventions.SaveEnumsAsIntegersForPatching = true;
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

            using (var session = store.OpenSession())
            {
                session.Advanced.Patch<Machine, Status>(id, x => x.Checks[Parts.Engine], Status.Bad);
                session.SaveChanges();
            }

            var patchedJson = GetRawJson(store, id);
            Assert.Equal("""{"Checks":{"Engine":2,"Gears":1}}""", patchedJson);
        }


        [RavenFact(RavenTestCategory.ClientApi)]
        public void PatchOnEnumPropertyAndSaveEnumsAsIntegersShouldBeInconsistent()
        {
            // https://github.com/ravendb/ravendb/pull/18530#discussion_r1612819842
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
                var entity = Thing.Create();
                session.Store(entity);
                session.SaveChanges();
                id = session.Advanced.GetDocumentId(entity);
            }

            var storedJson = GetRawJson(store, id);
            Assert.Equal("""{"Status":0}""", storedJson);

            using (var session = store.OpenSession())
            {
                session.Advanced.Patch<Thing, Status>(id, x => x.Status, Status.Bad);
                session.SaveChanges();
            }

            var patchedJson = GetRawJson(store, id);
            Assert.Equal("""{"Status":"Bad"}""", patchedJson);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void PatchOnEnumPropertyAndSaveEnumsAsIntegersAndSaveEnumsAsIntegersForPatchingShouldWork()
        {
            using var store = GetDocumentStore(new()
            {
                ModifyDocumentStore = store =>
                {
                    store.Conventions.SaveEnumsAsIntegers = true;
                    store.Conventions.SaveEnumsAsIntegersForPatching = true;
                },
            });

            string id;
            using (var session = store.OpenSession())
            {
                var entity = Thing.Create();
                session.Store(entity);
                session.SaveChanges();
                id = session.Advanced.GetDocumentId(entity);
            }

            var storedJson = GetRawJson(store, id);
            Assert.Equal("""{"Status":0}""", storedJson);

            using (var session = store.OpenSession())
            {
                session.Advanced.Patch<Thing, Status>(id, x => x.Status, Status.Bad);
                session.SaveChanges();
            }

            var patchedJson = GetRawJson(store, id);
            Assert.Equal("""{"Status":2}""", patchedJson);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void WithAddOrPatch()
        {
            using var store = GetDocumentStore(new()
            {
                ModifyDocumentStore = store =>
                {
                    store.Conventions.SaveEnumsAsIntegers = true;
                    store.Conventions.SaveEnumsAsIntegersForPatching = true;
                },
            });

            string id;
            var entity = Machine.Create();
            using (var session = store.OpenSession())
            {
                session.Store(entity);
                session.SaveChanges();
                id = session.Advanced.GetDocumentId(entity);
            }

            using (var session = store.OpenSession())
            {
                session.Advanced.AddOrPatch(id, entity, x => x.Checks[Parts.Breaks], Status.Good);
                session.SaveChanges();
            }

            var patchedJson = GetRawJson(store, id);
            Assert.Equal("""{"Checks":{"Engine":1,"Gears":1,"Breaks":1}}""", patchedJson);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void WithDictionaryAdderPatch()
        {
            using var store = GetDocumentStore(new()
            {
                ModifyDocumentStore = store =>
                {
                    store.Conventions.SaveEnumsAsIntegers = true;
                    store.Conventions.SaveEnumsAsIntegersForPatching = true;
                },
            });

            var entity = Machine.Create();
            string id;
            using (var session = store.OpenSession())
            {
                session.Store(entity);
                session.SaveChanges();
                id = session.Advanced.GetDocumentId(entity);
            }

            using (var session = store.OpenSession())
            {
                var item = session.Load<Machine>(id);
                session.Advanced.Patch(item, x => x.Checks, dict => dict.Add(Parts.Breaks, Status.Bad));
                session.SaveChanges();
            }
            using (var commands = store.Commands())
            {
                var item = commands.Get(id).BlittableJson;
                Assert.True(item.TryGet(nameof(Machine.Checks), out BlittableJsonReaderObject values));
                Assert.Equal(3, values.Count);

                Assert.True(values.TryGet(nameof(Parts.Engine), out int engineVal));
                Assert.Equal(1, engineVal);

                Assert.True(values.TryGet(nameof(Parts.Gears), out int gearsVal));
                Assert.Equal(1, gearsVal);

                Assert.True(values.TryGet(nameof(Parts.Breaks), out int breaksVal)); //fails, we get a string "Bad" instead of int 
                Assert.Equal(2, breaksVal);
            }
        }

        private string GetRawJson(DocumentStore store, string id)
        {
            using var session = store.OpenSession();
            using var stream = new MemoryStream();
            session.Advanced.LoadIntoStream([id], stream);
            stream.Position = 0;
            var response = JObject.Load(new JsonTextReader(new StreamReader(stream)));
            var results = response.GetValue("Results") as JArray;
            var single = results.Single() as JObject;
            single.Remove("@metadata");
            return single.ToString(Formatting.None);
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
            Breaks = 206,
        }

        private enum Status
        {
            None,
            Good,
            Bad,
        }

        private class Thing
        {
            public static Thing Create() => new()
            {
                Status = Status.None,
            };

            public Status Status { get; set; }
        }
    }
}
