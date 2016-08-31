// -----------------------------------------------------------------------
//  <copyright file="Maverix2.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Listeners;
using Raven.Server.Config;
using Xunit;

namespace SlowTests.MailingList
{
    public class Maverix2 : RavenTestBase
    {
        private readonly string _path;

        public Maverix2()
        {
            _path = NewDataPath();
        }

        public IDocumentStore Store()
        {
            var store = GetDocumentStore(path: _path);

            store.RegisterListener(new NonStaleQueryListener());
            store.Initialize();

            new TemplateTests_Search().Execute(store);

            return store;
        }

        [Fact]
        public void WithoutRestart()
        {
            var template = new TemplateTest();
            using (var store = Store())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(template);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(1, session.Query<TemplateTests_Search.ReduceResult, TemplateTests_Search>().Count());
                }
            }
        }

        [Fact]
        public void WithRestart()
        {
            var template = new TemplateTest();
            using (var store = Store())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(template);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(1, session.Query<TemplateTests_Search.ReduceResult, TemplateTests_Search>().Count());
                }
            }

            using (var store = Store())
            {
                using (var session = store.OpenSession())
                {
                    Assert.Equal(1, session.Query<TemplateTests_Search.ReduceResult, TemplateTests_Search>().Count());
                }
            }

        }

        private class NonStaleQueryListener : IDocumentQueryListener
        {
            public void BeforeQueryExecuted(IDocumentQueryCustomization customization)
            {
                customization.WaitForNonStaleResults();
            }
        }


        private class TemplateTest
        {
            public int Id { get; set; }
            public int masterId { get; set; }
            public string category { get; set; }
            public string name { get; set; }
            public string subject { get; set; }
            public string description { get; set; }
            public string text { get; set; }
            public string html { get; set; }

            public string thumbUrl { get; set; }
            public string previewUrl { get; set; }

            public string[] images { get; set; }
            public long size { get; set; }

            public int state { get; set; }
            public DateTime added { get; set; }
            public DateTime modified { get; set; }

            public int type { get; set; }
            public bool isAutomated { get; set; }
            public long stamp { get; set; }
        }

        private class TemplateTests_Search : AbstractIndexCreationTask<TemplateTest, TemplateTests_Search.ReduceResult>
        {
            public class ReduceResult
            {
                public string name { get; set; }
                public string category { get; set; }
                public string type { get; set; }
                public string[] targetIds { get; set; }
                public string state { get; set; }
                public DateTime added { get; set; }
                public string Query { get; set; }
            }

            public TemplateTests_Search()
            {
                Map = templates => from template in templates
                                   select new
                                   {
                                       name = template.name,
                                       category = template.category,
                                       type = template.type,
                                       state = template.state,
                                       added = template.added,
                                       Query = new object[]
                                   {
                                    template.name,
                                    template.category,
                                    template.subject,
                                    template.description
                                   }
                                   };
                Indexes.Add(x => x.Query, FieldIndexing.Analyzed);
            }
        }
    }
}
