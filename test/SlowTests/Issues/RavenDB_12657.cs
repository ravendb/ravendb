using System.IO;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12657 : RavenTestBase
    {
        [Fact]
        public void ShouldNotThrowConcurrencyException()
        {
            var store = GetDocumentStore();

            using (var session = store.OpenSession())
            using (var stream = new MemoryStream())
            {
                var ent = new EntAttachment { Name = "ent" };
                var ent2 = new EntAttachment { Name = "ent2" };
                var ent3 = new EntAttachment { Name = "ent3" };

                session.Store(ent);
                session.Store(ent2);
                session.Store(ent3);

                session.Advanced.Attachments.Store(ent, "name", stream);
                session.SaveChanges();

                session.Advanced.Attachments.Copy(ent, "name", ent2, "name");
                session.SaveChanges();

                ent.Name = "ent-new";
                ent2.Name = "ent2-new";
                ent3.Name = "ent3-new";

                session.SaveChanges();

                session.Advanced.Attachments.Move(ent, "name", ent3, "name");

                session.SaveChanges();

                ent.Name = "ent-new";
                ent2.Name = "ent2-new";
                ent3.Name = "ent3-new";

                session.SaveChanges();

                session.Advanced.Attachments.Delete(ent2, "name");

                session.SaveChanges();

                ent.Name = "ent-new-new";
                ent2.Name = "ent2-new-new";
                ent3.Name = "ent3-new-new";

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var ent = new EntAttachment { Name = "ent" };
                var ent2 = new EntAttachment { Name = "ent2" };

                session.Store(ent);
                session.Store(ent2);

                session.CountersFor(ent).Increment("Likes");
                session.CountersFor(ent).Increment("Dislikes");

                session.CountersFor(ent2).Increment("Likes");
                session.CountersFor(ent2).Increment("Dislikes");

                session.SaveChanges();

                ent.Name = "ent-new";
                ent2.Name = "ent2-new";

                session.SaveChanges();

                session.CountersFor(ent).Increment("Likes");
                session.CountersFor(ent).Increment("Dislikes");

                session.CountersFor(ent2).Increment("Likes");
                session.CountersFor(ent2).Increment("Dislikes");

                session.SaveChanges();

                ent.Name = "ent-new-new";
                ent2.Name = "ent2-new-new";

                session.SaveChanges();
            }
        }

        private class EntAttachment
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
