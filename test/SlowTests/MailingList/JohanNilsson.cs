﻿using FastTests;
using Xunit;
using System.Linq;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class JohanNilsson : RavenTestBase
    {
        public JohanNilsson(ITestOutputHelper output) : base(output)
        {
        }

        private interface IEntity
        {
            string Id2 { get; set; }
        }

        private interface IDomainObject : IEntity
        {
            string ImportantProperty { get; }
        }

        private class DomainObject : IDomainObject
        {
            public string Id2 { get; set; }
            public string ImportantProperty { get; set; }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void WithCustomizedTagNameAndIdentityProperty(Options options)
        {
            options.ModifyDocumentStore = s =>
            {
                var defaultFindIdentityProperty = s.Conventions.FindIdentityProperty;
                s.Conventions.FindIdentityProperty = property =>
                    typeof(IEntity).IsAssignableFrom(property.DeclaringType)
                        ? property.Name == "Id2"
                        : defaultFindIdentityProperty(property);

                s.Conventions.FindCollectionName = type =>
                    typeof(IDomainObject).IsAssignableFrom(type)
                        ? "domainobjects"
                        : DocumentConventions.DefaultGetCollectionName(type);
            };

            var id = string.Empty;
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var domainObject = new DomainObject();
                    session.Store(domainObject);
                    var domainObject2 = new DomainObject();
                    session.Store(domainObject2);
                    session.SaveChanges();
                    id = domainObject.Id2;
                }
                var matchingDomainObjects = store.OpenSession().Query<IDomainObject>().Where(_ => _.Id2 == id).ToList();
                Assert.Equal(matchingDomainObjects.Count, 1);
            }
        }
    }
}
