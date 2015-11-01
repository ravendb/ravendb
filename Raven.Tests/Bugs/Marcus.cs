using System.Linq;
using Raven.Client.Embedded;
using Xunit;

namespace Raven.Tests.Bugs
{
    public class Marcus 
    {
        public void CanQueryMetadata()
        {
            using (var store = new EmbeddableDocumentStore { RunInMemory = true })
            {
                store.Initialize();
                using (var s = store.OpenSession())
                {
                    s.Store(new User
                    {
                        Metadata =
                        {
                            IsActive = true
                        }
                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var actual = s.Query<User>()
                        .Customize(x=>x.WaitForNonStaleResultsAsOfLastWrite())
                        .Where(x => x.Metadata.IsActive == true)
                        .Count();
                    Assert.Equal(1, actual);
                }
            }
        }

        public class User
        {
            public Metadata Metadata { get; set; }
            public User()
            {
                Metadata = new Metadata();
            }
        }
        public class Metadata
        {
            public bool IsActive { get; set; }
        }
    }
}
