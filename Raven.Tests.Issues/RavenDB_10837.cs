using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_10837: RavenTest
    {
        [Fact]
        public void CanWaitForIndex()
        {
            var documentStore = NewDocumentStore();

            using (var session = documentStore.OpenSession())
            {
                session.Store(new Entity
                {
                    Id = "Entity/1"
                });
                session.Advanced.WaitForIndexesAfterSaveChanges();
                session.SaveChanges();
            }

            using (var session = documentStore.OpenSession())
            {
                var entities = session.Query<Entity>().ToList();

                Assert.NotEmpty(entities);
            }
        }
    }
}
