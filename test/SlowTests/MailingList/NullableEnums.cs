// //-----------------------------------------------------------------------
// // <copyright company="Hibernating Rhinos LTD">
// //     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// // </copyright>
// //-----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class NullableEnums : RavenTestBase
    {
        public NullableEnums(ITestOutputHelper output) : base(output)
        {
        }

        #region MonitorCategory enum

        private enum MonitorCategory
        {
            Normal,
            WideScreen
        }

        #endregion

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanQueryByEnum(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new ModelWithEnum
                    {
                        Category =
                            MonitorCategory.Normal
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var fromDb = session.Query<ModelWithEnum>().FirstOrDefault(m =>
                                                                               m.Category == MonitorCategory.Normal);
                    Assert.NotNull(fromDb);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanQueryByNullableEnum(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new ModelWithEnum
                    {
                        NullableCategory = MonitorCategory.Normal
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var fromDb = session.Query<ModelWithEnum>()
                        .FirstOrDefault(m => m.NullableCategory == MonitorCategory.Normal);
                    Assert.NotNull(fromDb);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanQueryByNullableEnumThatIsNull(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new ModelWithEnum { NullableCategory = null });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var fromDb = session.Query<ModelWithEnum>().FirstOrDefault(m =>
                                                                               m.NullableCategory == null);
                    Assert.NotNull(fromDb);
                }
            }
        }

        #region Nested type: ModelWithEnum

        private class ModelWithEnum
        {
            public MonitorCategory Category { get; set; }
            public MonitorCategory? NullableCategory { get; set; }
        }

        #endregion
    }
}
