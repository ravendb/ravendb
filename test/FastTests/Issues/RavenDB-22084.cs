using System.Collections.Generic;
using System.IO;
using System.Text;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_22084 : RavenTestBase
    {
        public RavenDB_22084(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void StoreOnDictionaryWithEnumKeyShouldWork()
        {
            using (var store = GetDocumentStore())
            {
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
        }

        [Fact]
        public void PatchOnDictionaryWithEnumKeyShouldWork()
        {
            using (var store = GetDocumentStore())
            {
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
                    session.Advanced.Patch<Machine, Status>(id, x => x.Checks[Type.Engine], Status.Bad);
                    session.SaveChanges();
                }

                var patchedJson = GetRawJson(store, id);
                Assert.Equal("""{"Checks":{"Engine":"Bad","Gears":"Good"}}""", patchedJson);
            }
        }

        private string GetRawJson(DocumentStore store, string id)
        {
            using var session = store.OpenSession();
            using var stream = new MemoryStream();
            session.Advanced.LoadIntoStream([id], stream);
            var json = Encoding.UTF8.GetString(stream.ToArray());
            return json.Substring(12, json.IndexOf(",\"@metadata") - 12) + "}";
        }

        public class Machine
        {
            public static Machine Create() => new()
            {
                Checks = new()
                {
                    { Type.Engine, Status.Good },
                    { Type.Gears, Status.Good },
                },
            };

            public Dictionary<Type, Status> Checks { get; set; }
        }

        public enum Type
        {
            Engine = 42,
            Gears = 102,
        }

        public enum Status
        {
            None,
            Good,
            Bad,
        }
    }
}
