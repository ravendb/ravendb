using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client;
using Raven.Client.Document;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Bugs.Identifiers
{
    public class SpecialCharactersOnIIS : WithNLog
    {
        public class Entity
        {
            public string Id { get; set; }
        }

        [Theory]
        [InlineData("foo")]
        [InlineData("SHA1-UdVhzPmv0o+wUez+Jirt0OFBcUY=")]
        public void Can_load_entity(string entityId)
        {
            var testContext = new IISClientTest();

            using(var store = testContext.GetDocumentStore())
            {
                store.Initialize();

                using (var session = store.OpenSession())
                {
                    var entity = new Entity() { Id = entityId };
                    session.Store(entity);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var entity1 = session.Load<object>(entityId);
                    Assert.NotNull(entity1);
                }
            }
        }
    }
}
