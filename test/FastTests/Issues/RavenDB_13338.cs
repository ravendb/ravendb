using System.Dynamic;
using System.Linq;
using Newtonsoft.Json.Linq;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_13338 : RavenTestBase
    {
        public RavenDB_13338(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void SkipAddingIdFieldToDynamicObjectShouldWork()
        {
            using (var store = GetDocumentStore(new Options()
            {
                ModifyDocumentStore = a => a.Conventions.AddIdFieldToDynamicObjects = false
            }))
            {
                using (var session = store.OpenSession())
                {
                    dynamic o = new ExpandoObject();
                    o.Name = "Grisha";
                    session.Store(o);

                    var entity = JObject.Parse(@"{ User: 1 }");
                    session.Store(entity);
                    Assert.Equal(1, entity.Properties().Count());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loadObject = session.Load<dynamic>("ExpandoObjects/1-A");
                    Assert.NotNull(loadObject);
                    Assert.Equal("Grisha", loadObject.Name.ToString());

                    var loadJObject = session.Load<dynamic>("JObjects/1-A");
                    Assert.NotNull(loadJObject);
                    Assert.Equal("1", loadJObject.User.ToString());
                    Assert.Null(loadJObject.Id);
                }
            }
        }

        [Fact]
        public void DynamicObjectIdShouldBeStrippedFromDocument()
        {
            const string expandoObjectId = "ExpandoObjects/1-A";
            const string jObjectId = "JObjects/1-A";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    dynamic o = new ExpandoObject();
                    o.Name = "Egor";
                    session.Store(o);

                    var entity = JObject.Parse(@"{ User: 1 }");
                    session.Store(entity);

                    // we add Id property by default
                    Assert.Equal(2, entity.Properties().Count());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loadObject = session.Load<dynamic>(expandoObjectId);
                    Assert.NotNull(loadObject);
                    Assert.Equal("Egor", loadObject.Name.ToString());
                    Assert.Equal(expandoObjectId, loadObject.Id.ToString());

                    var loadJObject = session.Load<dynamic>(jObjectId);
                    Assert.NotNull(loadJObject);
                    Assert.Equal("1", loadJObject.User.ToString());
                    Assert.Equal(jObjectId, loadJObject.Id.ToString());
                }

                using (var commands = store.Commands())
                {
                    const string idField = "Id";

                    var bJsonExpandoObject = commands.Get(expandoObjectId);
                    Assert.False(bJsonExpandoObject.ContainsKey(idField));

                    var bJsonJObject = commands.Get(jObjectId);
                    Assert.False(bJsonJObject.ContainsKey(idField));
                }
            }
        }
    }
}
