using System.Linq;
using FastTests;
using Raven.Client.Documents.Session;
using Raven.Client.Json.Serialization.JsonNet.Internal;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14234 : RavenTestBase
    {
        public RavenDB_14234(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void WhatChangedShouldNotThrowOnDerivedProperties()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var docOverride1 = new DocumentWithPropertyTypeOverride1();
                    session.Store(docOverride1);
                    var changes1 = session.Advanced.WhatChanged();
                    Assert.Equal(1, changes1.Count);
                    Assert.Equal(DocumentsChanges.ChangeType.DocumentAdded, changes1.First().Value.FirstOrDefault().Change);

                    var docOverride2 = new DocumentWithPropertyTypeOverride2();
                    session.Store(docOverride2);
                    var changes2 = session.Advanced.WhatChanged();
                    Assert.Equal(2, changes2.Count);

                    foreach (var change in changes2)
                    {
                        Assert.Equal(DocumentsChanges.ChangeType.DocumentAdded, change.Value.FirstOrDefault().Change);
                    }

                    var docOverride3 = new DocumentWithPropertyTypeOverride3();
                    session.Store(docOverride3);
                    var changes3 = session.Advanced.WhatChanged();
                    Assert.Equal(3, changes3.Count);

                    foreach (var change in changes3)
                    {
                        Assert.Equal(DocumentsChanges.ChangeType.DocumentAdded, change.Value.FirstOrDefault().Change);
                    }

                    var docOverride4 = new DocumentWithPropertyTypeOverride4();
                    session.Store(docOverride4);
                    var changes4 = session.Advanced.WhatChanged();
                    Assert.Equal(4, changes4.Count);

                    foreach (var change in changes4)
                    {
                        Assert.Equal(DocumentsChanges.ChangeType.DocumentAdded, change.Value.FirstOrDefault().Change);
                    }

                    var docOverride5 = new IDocumentWithPropertyTypeOverride5();
                    session.Store(docOverride5);
                    var changes5 = session.Advanced.WhatChanged();
                    Assert.Equal(5, changes5.Count);

                    foreach (var change in changes5)
                    {
                        Assert.Equal(DocumentsChanges.ChangeType.DocumentAdded, change.Value.FirstOrDefault().Change);
                    }
                    session.SaveChanges();
                }
            }
        }

        [Fact]
        public void ShouldAssignTheTypeOfDerivedProperties()
        {
            var entity5 = new IDocumentWithPropertyTypeOverride5();
            var type5 = BlittableJsonConverter.GetPropertyType(nameof(entity5.Reference), entity5.GetType());
            Assert.Equal(typeof(IRefDerived_Class), type5);

            var entity4 = new DocumentWithPropertyTypeOverride4();
            var type4 = BlittableJsonConverter.GetPropertyType(nameof(entity4.Reference), entity4.GetType());
            Assert.Equal(typeof(RefDerived3), type4);

            var entity3 = new DocumentWithPropertyTypeOverride3();
            var type3 = BlittableJsonConverter.GetPropertyType(nameof(entity3.Reference), entity3.GetType());
            Assert.Equal(typeof(RefDerived3), type3);

            var entity2 = new DocumentWithPropertyTypeOverride2();
            var type2 = BlittableJsonConverter.GetPropertyType(nameof(entity2.Reference), entity2.GetType());
            Assert.Equal(typeof(RefDerived2), type2);

            var entity1 = new DocumentWithPropertyTypeOverride1();
            var type1 = BlittableJsonConverter.GetPropertyType(nameof(entity1.Reference), entity1.GetType());
            Assert.Equal(typeof(RefDerived2), type1);

            var entity0 = new DocumentBase();
            var type0 = BlittableJsonConverter.GetPropertyType(nameof(entity0.Reference), entity0.GetType());
            Assert.Equal(typeof(RefDerived1), type0);
        }

        [Fact]
        public void ShouldAssignTheTypeOfDerivedPropertiesWithInterface()
        {
            var entity5 = new IDocumentWithPropertyTypeOverride5();
            var type5 = BlittableJsonConverter.GetPropertyType(nameof(entity5.Reference), entity5.GetType());
            Assert.Equal(typeof(IRefDerived_Class), type5);

            var entity4 = new IDocumentWithPropertyTypeOverride4();
            var type4 = BlittableJsonConverter.GetPropertyType(nameof(entity4.Reference), entity4.GetType());
            Assert.Equal(typeof(IRefDerived3), type4);

            var entity3 = new IDocumentWithPropertyTypeOverride3();
            var type3 = BlittableJsonConverter.GetPropertyType(nameof(entity3.Reference), entity3.GetType());
            Assert.Equal(typeof(IRefDerived3), type3);

            var entity2 = new IDocumentWithPropertyTypeOverride2();
            var type2 = BlittableJsonConverter.GetPropertyType(nameof(entity2.Reference), entity2.GetType());
            Assert.Equal(typeof(IRefDerived2), type2);

            var entity1 = new IDocumentWithPropertyTypeOverride1();
            var type1 = BlittableJsonConverter.GetPropertyType(nameof(entity1.Reference), entity1.GetType());
            Assert.Equal(typeof(IRefDerived2), type1);

            var entity0 = new IDocumentWithPropertyTypeOverrideBase();
            var type0 = BlittableJsonConverter.GetPropertyType(nameof(entity0.Reference), entity0.GetType());
            Assert.Equal(typeof(IRefBase), type0);
        }

        private class RefBase
        {
        }

        private class RefDerived1 : RefBase
        {
        }

        private class RefDerived2 : RefBase
        {
        }

        private class RefDerived3 : RefBase
        {
        }

        private class DocumentBase
        {
            public string Id { get; set; }
            public RefDerived1 Reference { get; set; }
        }

        private class DocumentWithPropertyTypeOverride1 : DocumentBase
        {
            public new RefDerived2 Reference { get; set; }
        }

        private class DocumentWithPropertyTypeOverride2 : DocumentWithPropertyTypeOverride1
        {
        }

        private class DocumentWithPropertyTypeOverride3 : DocumentWithPropertyTypeOverride2
        {
            public new RefDerived3 Reference { get; set; }
        }

        private class DocumentWithPropertyTypeOverride4 : DocumentWithPropertyTypeOverride3
        {
        }

        private interface IDocumentBase
        {
            IRefBase Reference { get; set; }
        }

        private interface IRefBase
        {
        }

        private class IRefDerived2 : IRefBase
        {
        }

        private class IRefDerived3 : IRefBase
        {
        }

        private class IDocumentWithPropertyTypeOverrideBase : IDocumentBase
        {
            public IRefBase Reference { get; set; }
        }

        private class IDocumentWithPropertyTypeOverride1 : IDocumentWithPropertyTypeOverrideBase
        {
            public new IRefDerived2 Reference { get; set; }
        }

        private class IDocumentWithPropertyTypeOverride2 : IDocumentWithPropertyTypeOverride1
        {
        }

        private class IDocumentWithPropertyTypeOverride3 : IDocumentWithPropertyTypeOverride2
        {
            public new IRefDerived3 Reference { get; set; }
        }

        private class IDocumentWithPropertyTypeOverride4 : IDocumentWithPropertyTypeOverride3
        {
        }

        private class IRefDerived_Class : IRefDerived2
        {
        }

        private class IDocumentWithPropertyTypeOverride5 : IDocumentWithPropertyTypeOverride2
        {
            public new IRefDerived_Class Reference { get; set; }
        }
    }
}
