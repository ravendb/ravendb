using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database.Indexing;
using Xunit;
using System.Linq;

namespace Raven.Client.Tests.Bugs
{
    public class Translators : LocalClientTest
    {
        [Fact]
        public void CanUseTranslatorToModifyQueryResults()
        {
            using(var ds = NewDocumentStore())
            {
                using(var s = ds.OpenSession())
                {
                    s.Store(new User {Name = "Ayende"});
                    s.SaveChanges();
                }

                ds.DatabaseCommands.PutIndex("Users",
                                             new IndexDefinition
                                             {
                                                 Map = "from u in docs.Users select new { u.Name }",
                                                 Translator = "from user in results select new { Name = user.Name.ToUpper() }"
                                             });


                using (var s = ds.OpenSession())
                {
                    var first = s.Query<JObject>("Users").Customize(x=>x.WaitForNonStaleResults())
                        .First();

                    Assert.Equal("AYENDE", first.Value<string>("Name"));
                }
            }
        }

        [Fact]
        public void CanUseTranslatorToLoadAnotherDocument()
        {
            using (var ds = NewDocumentStore())
            {
                using (var s = ds.OpenSession())
                {
                    var entity = new User { Name = "Ayende" };
                    s.Store(entity);
                    s.Store(new User { Name = "Oren", PartnerId = entity.Id});
                    s.SaveChanges();
                }

                ds.DatabaseCommands.PutIndex("Users",
                                             new IndexDefinition
                                             {
                                                 Map = "from u in docs.Users select new { u.Name }",
                                                 Translator = "from user in results select new { Name = user.Name, Partner = Database.Load(user.PartnerId).Name }"
                                             });


                using (var s = ds.OpenSession())
                {
                    var first = s.LuceneQuery<JObject>("Users")
                        .WaitForNonStaleResults()
                        .WhereEquals("Name", "Oren", true)
                        .First();

                    Assert.Equal(@"{""Name"":""Oren"",""Partner"":""Ayende""}", first.ToString(Formatting.None));
                }
            }
        }
    }
}