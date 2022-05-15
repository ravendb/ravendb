using System.Collections.Generic;
using FastTests;
using FastTests.Server.JavaScript;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12248 : RavenTestBase
    {
        public RavenDB_12248(ITestOutputHelper output) : base(output)
        {
        }

        private class BaseClass
        {
            public string Name { get; set; }
        }

        private class SubClass : BaseClass
        {
        }

        private class BaseClassContainer
        {
            public BaseClass BaseClass { get; set; }
        }

        private class Entity
        {
            public string Id { get; set; }
            public BaseClass Property { get; set; }
            public BaseClassContainer Container { get; set; }
            public List<BaseClassContainer> List { get; set; }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void PatchShouldRespectTypeOfSubclass(Options options)
        {
            var entity = new Entity
            {
                Property = new BaseClass
                {
                    Name = "BaseClass"
                }
            };

            using (var store = GetDocumentStore(options))
            {             
                using (var session = store.OpenSession())
                {
                    session.Store(entity);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    BaseClass newProperty = new SubClass
                    {
                        Name = "SubClass"
                    };

                    session.Advanced.Patch<Entity, BaseClass>(entity.Id, x => x.Property, (SubClass)newProperty);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loadedEntity = session.Load<Entity>(entity.Id);

                    Assert.NotNull(loadedEntity.Property as SubClass);
                    Assert.Equal("SubClass", ((SubClass)loadedEntity.Property).Name);
                }

            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void PatchOnNestedPropertyShouldRespectTypeOfSubclass(Options options)
        {
            var entity = new Entity
            {
                Container = new BaseClassContainer
                {
                    BaseClass = new BaseClass
                    {
                        Name = "BaseClass"
                    }
                }
            };

            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(entity);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    BaseClass newProperty = new SubClass
                    {
                        Name = "SubClass"
                    };

                    session.Advanced.Patch<Entity, BaseClass>(entity.Id, x => x.Container.BaseClass, (SubClass)newProperty);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loadedEntity = session.Load<Entity>(entity.Id);

                    Assert.NotNull(loadedEntity.Container.BaseClass as SubClass);
                    Assert.Equal("SubClass", ((SubClass)loadedEntity.Container.BaseClass).Name);
                }

            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void PatchOnArrayElementShouldRespectTypeOfSubclass(Options options)
        {
            var entity = new Entity
            {
                List = new List<BaseClassContainer>
                {
                    new BaseClassContainer
                    {
                        BaseClass = new BaseClass
                        {
                            Name = "BaseClass1"
                        }
                    },
                    new BaseClassContainer
                    {
                        BaseClass = new BaseClass
                        {
                            Name = "BaseClass2"
                        }
                    }
                }
            };

            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(entity);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    BaseClass newProperty = new SubClass
                    {
                        Name = "SubClass1"
                    };

                    session.Advanced.Patch<Entity, BaseClass>(entity.Id, x => x.List[0].BaseClass, (SubClass)newProperty);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loadedEntity = session.Load<Entity>(entity.Id);

                    Assert.NotNull(loadedEntity.List[0].BaseClass as SubClass);
                    Assert.Equal("SubClass1", ((SubClass)loadedEntity.List[0].BaseClass).Name);

                    Assert.Equal(2, loadedEntity.List.Count);
                    Assert.Equal("BaseClass2", loadedEntity.List[1].BaseClass.Name);

                }

            }
        }
    }
}
