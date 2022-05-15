// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4144.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using FastTests;
using FastTests.Server.JavaScript;
using Raven.Client.Documents.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_4144 : RavenTestBase
    {
        public RavenDB_4144(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void can_save_javascript_array_values(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var foo = new Foo
                {
                    List = { "test" }
                };

                using (var session = store.OpenSession())
                {
                    session.Store(foo);
                    session.SaveChanges();
                }

                store.Operations.Send(new PatchOperation(foo.Id, null, new PatchRequest
                {
                    Script = @"var list = ['my', 'list']; 
                                for(var x in list){
                                        if(list.hasOwnProperty(x) == false) continue;
                                    this.List[x] = list[x];
                                }"
                }));

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<Foo>(foo.Id);
                    Assert.Equal(new List<string> { "my", "list" }, loaded.List);
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void can_use_non_existing_function1(Options options)
        {
            using (var store = GetDocumentStore(options))
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

                store.Operations.Send(new PatchOperation(foo.Id, null, new PatchRequest
                {
                    Script = @"var test = ['My', 'Array'];
                               this.Name = test.RemoveWhere;"
                }));

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<Foo>(foo.Id);
                    Assert.Null(loaded.Name);
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void can_use_non_existing_function2(Options options)
        {
            using (var store = GetDocumentStore(options))
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

                store.Operations.Send(new PatchOperation(foo.Id, null, new PatchRequest
                {
                    Script = @"var test = ['My', 'Array'];
                               this.Name = function() {}"
                }));

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<Foo>(foo.Id);
                    Assert.Null(loaded.Name);
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void can_use_non_existing_function3(Options options)
        {
            using (var store = GetDocumentStore(options))
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

                store.Operations.Send(new PatchOperation(foo.Id, null, new PatchRequest
                {
                    Script = @"function myConverter(str) {
                                  return str + ' whoeeehoeee'
                               }
                               this.Name = myConverter;"
                }));

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<Foo>(foo.Id);
                    Assert.Null(loaded.Name);
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void can_evaluate_function(Options options)
        {
            using (var store = GetDocumentStore(options))
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

                store.Operations.Send(new PatchOperation(foo.Id, null, new PatchRequest
                {
                    Script = @"function myConverter(str) {
                                  return str + ' whoeeehoeee'
                               }
                               this.Name = myConverter(this.Name);"
                }));

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<Foo>(foo.Id);
                    Assert.Equal("test whoeeehoeee", loaded.Name);
                }
            }
        }

        private class Foo
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
