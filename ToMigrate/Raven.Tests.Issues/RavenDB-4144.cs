// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4144.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4144 : RavenTest
    {
        [Fact]
        public void can_save_javascript_array_values()
        {
            using (var store = NewDocumentStore())
            {
                var foo = new Foo
                {
                    List = {"test"}
                };

                using (var session = store.OpenSession())
                {
                    session.Store(foo);
                    session.SaveChanges();
                }

                store.DatabaseCommands.Patch(foo.Id, new ScriptedPatchRequest
                {
                    Script = @"var list = ['my', 'list']; 
                                for(var x in list){
                                    this.List[x] = list[x];
                                }"
                });

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<Foo>(foo.Id);
                    Assert.Equal(new List<string> {"my", "list"}, loaded.List);
                }
            }
        }

        [Fact]
        public void can_use_non_existing_function1()
        {
            using (var store = NewDocumentStore())
            {
                var foo = new Foo
                {
                    Name = "test"
                };

                using (var session = store.OpenSession())
                {
                    session.Store(foo);
                    session.SaveChanges();
                }

                store.DatabaseCommands.Patch(foo.Id, new ScriptedPatchRequest
                {
                    Script = @"var test = ['My', 'Array'];
                               this.Name = test.RemoveWhere;"
                });

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<Foo>(foo.Id);
                    Assert.Null(loaded.Name);
                }
            }
        }

        [Fact]
        public void can_use_non_existing_function2()
        {
            using (var store = NewDocumentStore())
            {
                var foo = new Foo
                {
                    Name = "test"
                };

                using (var session = store.OpenSession())
                {
                    session.Store(foo);
                    session.SaveChanges();
                }

                store.DatabaseCommands.Patch(foo.Id, new ScriptedPatchRequest
                {
                    Script = @"var test = ['My', 'Array'];
                               this.Name = function() {}"
                });

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<Foo>(foo.Id);
                    Assert.Null(loaded.Name);
                }
            }
        }

        [Fact]
        public void can_use_non_existing_function3()
        {
            using (var store = NewDocumentStore())
            {
                var foo = new Foo
                {
                    Name = "test"
                };

                using (var session = store.OpenSession())
                {
                    session.Store(foo);
                    session.SaveChanges();
                }

                store.DatabaseCommands.Patch(foo.Id, new ScriptedPatchRequest
                {
                    Script = @"function myConverter(str) {
                                  return str + ' whoeeehoeee'
                               }
                               this.Name = myConverter;"
                });

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<Foo>(foo.Id);
                    Assert.Null(loaded.Name);
                }
            }
        }

        [Fact]
        public void can_evaluate_function()
        {
            using (var store = NewDocumentStore())
            {
                var foo = new Foo
                {
                    Name = "test"
                };

                using (var session = store.OpenSession())
                {
                    session.Store(foo);
                    session.SaveChanges();
                }

                store.DatabaseCommands.Patch(foo.Id, new ScriptedPatchRequest
                {
                    Script = @"function myConverter(str) {
                                  return str + ' whoeeehoeee'
                               }
                               this.Name = myConverter(this.Name);"
                });

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<Foo>(foo.Id);
                    Assert.Equal("test whoeeehoeee", loaded.Name);
                }
            }
        }

        public class Foo
        {
            public Foo()
            {
                List = new List<string>();    
            }

            public string Id { get; set; }

            public List<string> List { get; set; }

            public string Name { get; set; }
        }
    }
}
