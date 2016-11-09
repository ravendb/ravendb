using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Linq;
using Raven.Tests.Core.Utils.Entities;
using Raven.Tests.Core.Utils.Indexes;
using Xunit;

namespace Raven.Tests.Core.Querying
{
    public class Escaping : RavenCoreTestBase
    {
#if DNXCORE50
        public Escaping(TestServerFixture fixture)
            : base(fixture)
        {

        }
#endif

        [Fact]
        public void CanPerformQueryOnEscapedText()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store( new Employee() {Name = "Grisha Kotler", WorksAt = "Hibernating - Rhinos" });
                    session.SaveChanges();

                    var employee = session.Query<Employee>().Customize(x=>x.WaitForNonStaleResults()).FirstOrDefault(x => x.WorksAt.Equals("Hibernating - Rhinos"));
                    Assert.Equal("Grisha Kotler", employee?.Name);
                }                            
            }
        }
        [Fact]
        public void CanPerformInQueryWithTermsStartingWithComment()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee() { Name = "Grisha Kotler", WorksAt = "Hibernating - Rhinos" });
                    session.SaveChanges();                    
                    var employee = session.Query<Employee>().Customize(x => x.WaitForNonStaleResults()).FirstOrDefault(x => x.WorksAt.In( "a","//b","Hibernating - Rhinos","c"));
                    Assert.Equal("Grisha Kotler", employee?.Name);
                }
            }
        }

        public class Employee
        {
            public string Name { get; set; }
            public string WorksAt { get; set; }
        }
    }
}