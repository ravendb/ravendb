// //-----------------------------------------------------------------------
// // <copyright company="Hibernating Rhinos LTD">
// //     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// // </copyright>
// //-----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.MailingList
{
    public class NullableEnums : RavenTestBase
    {
        #region MonitorCategory enum

        private enum MonitorCategory
        {
            Normal,
            WideScreen
        }

        #endregion

        [Fact]
        public void CanQueryByEnum()
        {
            using (var store = GetDocumentStore())
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

        [Fact]
        public void CanQueryByNullableEnum()
        {
            using (var store = GetDocumentStore())
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

        [Fact]
        public void CanQueryByNullableEnumThatIsNull()
        {
            using (var store = GetDocumentStore())
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
