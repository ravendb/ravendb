using Raven.Tests.Core.Utils.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Core.Session
{
    public class Advanced : RavenCoreTestBase
    {
        [Fact]
        public void CanGetDocumentUrl()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Id = "companies/1" });
                    session.SaveChanges();

                    var company = session.Load<Company>("companies/1");
                    Assert.NotNull(company);
                    var uri = new Uri(session.Advanced.GetDocumentUrl(company));
                    Assert.Equal("/databases/"+store.DefaultDatabase+"/docs/companies/1", uri.AbsolutePath);
                }
            }
        }
    }
}
