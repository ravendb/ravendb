using System.Linq;
using System.Threading;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB1261 : RavenTestBase
    {
        public class Student
        {
            public string Email { get; set; }
        }

        public class StudentIndex : AbstractIndexCreationTask<Student>
        {
            public StudentIndex()
            {
                Map = students => from s in students
                                  select new
                                  {
                                      s.Email
                                  };
            }
        }

        [Fact]
        public void Run()
        {
            using (var store = NewDocumentStore())
            {
                new StudentIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Student { Email = "support@hibernatingrhinos.com" });
                    session.SaveChanges();
                }

                while (store.DatabaseCommands.GetStatistics().StaleIndexes.Any())
                    Thread.Sleep(10);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Student, StudentIndex>();

                    var stream = session.Advanced.Stream(query);

                    stream.MoveNext();

                    Assert.NotNull(stream.Current.Key);
                }
            }
        }
    }
}

