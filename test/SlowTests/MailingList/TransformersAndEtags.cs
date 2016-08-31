// -----------------------------------------------------------------------
//  <copyright file="TransformersAndEtags.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class TransformersAndEtags : RavenTestBase
    {
        private class Company
        {
            public string Name { get; set; }
            public string Parent { get; set; }
        }

        private class Item
        {
            public string[] Names { get; set; }
        }

        private class AllNames : AbstractTransformerCreationTask<Company>
        {
            public AllNames()
            {
                TransformResults = companies =>
                                   from company in companies
                                   select new
                                   {
                                       Names =
                                       company.Parent == null
                                           ? new[] { company.Name }
                                           : new[] { company.Name, LoadDocument<Company>(company.Parent).Name }
                                   };
            }
        }

        [Fact(Skip = "https://github.com/dotnet/roslyn/issues/12045")]
        public void CanGetUpdatesForChangedRelation()
        {
            using (var store = GetDocumentStore())
            {
                new AllNames().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "A" });
                    session.Store(new Company { Name = "B", Parent = "companies/1" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var load = session.Load<AllNames, Item>("companies/2");
                    Assert.Equal(new[]{"B","A"}, load.Names);
                }

                using (var session = store.OpenSession())
                {
                    session.Load<Company>(1).Name = "C";
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var load = session.Load<AllNames, Item>("companies/2");
                    Assert.Equal(new[] { "B", "C" }, load.Names);
                }

            }
        }
    }
}
