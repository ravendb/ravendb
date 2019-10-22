using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11649_ProvideContextForNestedFieldsChanges : RavenTestBase
    {
        public RavenDB_11649_ProvideContextForNestedFieldsChanges(ITestOutputHelper output) : base(output)
        {
        }

        class OuterClass
        {
            public InnerClass[][] InnerClassMatrix { get; set; }
            public InnerClass[] InnerClasses { get; set; }
            public string A { get; set; }
            public InnerClass InnerClass { get; set; }
            public MiddleClass MiddleClass { get; set; }
        }

        class InnerClass
        {
            public string A { get; set; }
        }

        class MiddleClass
        {
            public InnerClass A { get; set; }
        }

        [Fact]
        public void WhatChanged_WhenInnerPropertyChanged_ShouldReturnThePropertyNamePlusPath()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    //Arrange
                    var doc = new OuterClass
                    {
                        A = "outerValue",
                        InnerClass = new InnerClass
                        {
                            A = "innerValue",
                        }
                    };
                    const string id = "docs/1";
                    session.Store(doc, id);
                    session.SaveChanges();

                    doc.InnerClass.A = "newInnerValue";

                    //Action
                    var changes = session.Advanced.WhatChanged();

                    //Assert
                    var changedPaths = changes[id]
                        .Select(c => c.FieldPath)
                        .ToList();

                    var pathsExpected = new[] { "InnerClass" };
                    RavenTestHelper.AreEquivalent(pathsExpected, changedPaths);
                }
            }
        }

        [Fact]
        public void WhatChanged_WhenInnerPropertyChangedFromNull_ShouldReturnThePropertyNamePlusPath()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    //Arrange
                    var doc = new OuterClass
                    {
                        A = "outerValue",
                        InnerClass = new InnerClass
                        {
                            A = null
                        }
                    };
                    const string id = "docs/1";
                    session.Store(doc, id);
                    session.SaveChanges();

                    doc.InnerClass.A = "newInnerValue";

                    //Action
                    var changes = session.Advanced.WhatChanged();

                    //Assert
                    var changedPaths = changes[id]
                        .Select(c => c.FieldPath)
                        .ToList();

                    var pathsExpected = new[] { "InnerClass" };
                    RavenTestHelper.AreEquivalent(pathsExpected, changedPaths);
                }
            }
        }

        [Fact]
        public void WhatChanged_WhenPropertyOfInnerPropertyChangedToNull_ShouldReturnThePropertyNamePlusPath()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    //Arrange
                    var doc = new OuterClass
                    {
                        A = "outerValue",
                        InnerClass = new InnerClass
                        {
                            A = "innerValue",
                        }
                    };
                    const string id = "docs/1";
                    session.Store(doc, id);
                    session.SaveChanges();

                    doc.InnerClass.A = null;

                    //Action
                    var changes = session.Advanced.WhatChanged();

                    //Assert
                    var changedPaths = changes[id]
                        .Select(c => c.FieldPath)
                        .ToList();

                    var pathsExpected = new[] { "InnerClass" };
                    RavenTestHelper.AreEquivalent(pathsExpected, changedPaths);
                }
            }
        }

        [Fact]
        public void WhatChanged_WhenOuterPropertyChanged_FieldPathShouldBeEmpty()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    //Arrange
                    var doc = new OuterClass()
                    {
                        A = "outerValue",
                        InnerClass = new InnerClass
                        {
                            A = "innerValue",
                        }
                    };
                    const string id = "docs/1";
                    session.Store(doc, id);
                    session.SaveChanges();

                    doc.A = "newOuterValue";

                    //Action
                    var changes = session.Advanced.WhatChanged();

                    //Assert
                    var changedPaths = changes[id]
                        .Select(c => c.FieldPath)
                        .ToList();

                    var pathsExpected = new[] { "" };
                    RavenTestHelper.AreEquivalent(pathsExpected, changedPaths);
                }
            }
        }

        [Fact]
        public void WhatChanged_WhenInnerPropertyInArrayChanged_ShouldReturnWithRelevantPath()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    //Arrange
                    var doc = new OuterClass()
                    {
                        A = "outerValue",
                        InnerClasses = new[]
                        {
                            new InnerClass
                            {
                                A = "innerValue",
                            }
                        }
                    };
                    const string id = "docs/1";
                    session.Store(doc, id);
                    session.SaveChanges();

                    doc.InnerClasses[0].A = "newInnerValue";

                    //Action
                    var changes = session.Advanced.WhatChanged();

                    //Assert
                    var changedPaths = changes[id]
                        .Select(c => c.FieldPath)
                        .ToList();

                    var pathsExpected = new[] { "InnerClasses[0]" };
                    RavenTestHelper.AreEquivalent(pathsExpected, changedPaths);
                }
            }
        }

        [Fact]
        public void WhatChanged_WhenArrayPropertyInArrayChangedFromNull_ShouldReturnWithRelevantPath()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    //Arrange
                    var doc = new OuterClass()
                    {
                        InnerClassMatrix = new InnerClass[1][]
                    };
                    const string id = "docs/1";
                    session.Store(doc, id);
                    session.SaveChanges();

                    doc.InnerClassMatrix[0] = new[] { new InnerClass() };

                    //Action
                    var changes = session.Advanced.WhatChanged();

                    //Assert
                    var changedPaths = changes[id]
                        .Select(c => c.FieldPath)
                        .ToList();

                    var pathsExpected = new[] { "InnerClassMatrix[0]" };
                    RavenTestHelper.AreEquivalent(pathsExpected, changedPaths);
                }
            }
        }

        [Fact]
        public void WhatChanged_WhenInMatrixChanged_ShouldReturnWithRelevantPath()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    //Arrange
                    var doc = new OuterClass()
                    {
                        InnerClassMatrix = new[] { new[] { new InnerClass { A = "oldValue" } } }
                    };
                    const string id = "docs/1";
                    session.Store(doc, id);
                    session.SaveChanges();

                    doc.InnerClassMatrix[0][0].A = "newValue";

                    //Action
                    var changes = session.Advanced.WhatChanged();

                    //Assert
                    var changedPaths = changes[id]
                        .Select(c => c.FieldPath)
                        .ToList();

                    var pathsExpected = new[] { "InnerClassMatrix[0][0]" };
                    RavenTestHelper.AreEquivalent(pathsExpected, changedPaths);
                }
            }
        }

        [Fact]
        public void WhatChanged_WhenAllNamedAPropertiesChanged_ShouldReturnDifferentPaths()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    //Arrange
                    var doc = new OuterClass()
                    {
                        A = "outerValue",
                        InnerClass = new InnerClass
                        {
                            A = "innerValue",
                        },
                        MiddleClass = new MiddleClass
                        {
                            A = null
                        },
                        InnerClasses = new[] { new InnerClass { A = "oldValue" } },
                        InnerClassMatrix = new[] { new[] { new InnerClass { A = "oldValue" } } }
                    };
                    const string id = "docs/1";
                    session.Store(doc, id);
                    session.SaveChanges();

                    doc.A = "newOuterValue";
                    doc.InnerClass.A = "newInnerValue";
                    doc.MiddleClass.A = new InnerClass();
                    doc.InnerClasses[0].A = "newValue";
                    doc.InnerClassMatrix[0][0].A = "newValue";

                    //Action
                    var changes = session.Advanced.WhatChanged();

                    //Assert
                    var changedPaths = changes[id]
                        .Select(c => c.FieldPath)
                        .ToList();

                    var pathsExpected = new[]
                    {
                        "",
                        "InnerClass",
                        "MiddleClass",
                        "InnerClasses[0]",
                        "InnerClassMatrix[0][0]"
                    };
                    RavenTestHelper.AreEquivalent(pathsExpected, changedPaths);
                }
            }
        }
    }
}
