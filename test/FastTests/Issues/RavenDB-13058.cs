using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_13058 : RavenTestBase
    {
        public RavenDB_13058(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
#pragma warning disable 649
            public string Name;
            public bool Flag;
#pragma warning restore 649
        }

        private static bool IsValid(User u) => true;

        private static bool IsValid2(User u, bool flag) => true;

        private class IndexWithCustomMethod : AbstractIndexCreationTask<User>
        {
            public IndexWithCustomMethod()
            {
                Map = users => from u in users
                               where IsValid(u)
                               select new { u.Name };

                AdditionalSources = new Dictionary<string, string>()
                {
                    {
                        "RavenDB_13058",
                        @"
                        namespace FastTests.Issues
                        {
                            public class RavenDB_13058
                            {
                                public class User
                                {
                                    public string Name { get; set; }
                                }

                                public static bool IsValid(User u) { return u.Name != ""ayende""; }
                            }
                        }
                        "
                    }
                };
            }
        }

        private class IndexWithArrowFunctionStaticParameter : AbstractIndexCreationTask<User>
        {
            public IndexWithArrowFunctionStaticParameter()
            {
                Map = users => from u in users
                               where IsValid(u)
                               select new { u.Name };

                AdditionalSources = new Dictionary<string, string>()
                {
                    {
                        "RavenDB_13058",
                        @"
                        namespace FastTests.Issues
                        {
                            public class RavenDB_13058
                            {
                                public class User
                                {
                                    public string Name { get; set; }
                                }

                                public static bool IsValid(User u) => u.Name != ""ayende"";
                            }
                        }
                        "
                    }
                };
            }
        }

        private class IndexWithInvalidParameterCount : AbstractIndexCreationTask<User>
        {
            public IndexWithInvalidParameterCount()
            {
                Map = users => from u in users
                               where IsValid(u)
                               select new { u.Name };

                AdditionalSources = new Dictionary<string, string>()
                {
                    {
                        "RavenDB_13058",
                        @"
                        namespace FastTests.Issues
                        {
                            public class RavenDB_13058
                            {
                                public class User
                                {
                                    public string Name { get; set; }
                                }

                                public static bool IsValid(User u, User u2) => u.Name != ""ayende"";
                            }
                        }
                        "
                    }
                };
            }
        }

        private class IndexWithInvalidReturnType : AbstractIndexCreationTask<User>
        {
            public IndexWithInvalidReturnType()
            {
                Map = users => from u in users
                               where IsValid(u)
                               select new { u.Name };

                AdditionalSources = new Dictionary<string, string>()
                {
                    {
                        "RavenDB_13058",
                        @"
                        namespace FastTests.Issues
                        {
                            public class RavenDB_13058
                            {
                                public class User
                                {
                                    public string Name { get; set; }
                                }

                                public static User IsValid(User u) => u;
                            }
                        }
                        "
                    }
                };
            }
        }

        private class IndexWithArrowFunctionDynamicParameter : AbstractIndexCreationTask<User>
        {
            public IndexWithArrowFunctionDynamicParameter()
            {
                Map = users => from u in users
                               where IsValid(u)
                               select new { u.Name };

                AdditionalSources = new Dictionary<string, string>()
                {
                    {
                        "RavenDB_13058",
                        @"
                        namespace FastTests.Issues
                        {
                            public class RavenDB_13058
                            {
                                public class User
                                {
                                    public string Name { get; set; }
                                }

                                public static bool IsValid(dynamic u) => u.Name != ""ayende"";
                            }
                        }
                        "
                    }
                };
            }
        }

        private class IndexWithArrowFunctionDynamicAndStaticParameter : AbstractIndexCreationTask<User>
        {
            public IndexWithArrowFunctionDynamicAndStaticParameter()
            {
                Map = users => from u in users
                               where IsValid2(u, u.Flag)
                               select new { u.Name };

                AdditionalSources = new Dictionary<string, string>()
                {
                    {
                        "RavenDB_13058",
                        @"
                        namespace FastTests.Issues
                        {
                            public class RavenDB_13058
                            {
                                public class User
                                {
                                    public string Name { get; set; }
                                    public bool Flag { get; set; }
                                }

                                public static bool IsValid2(dynamic u, bool flag) => u.Name != ""ayende"" && flag;
                            }
                        }
                        "
                    }
                };
            }
        }

        [Fact]
        public void Can_use_custom_method_in_index()
        {
            using (var store = GetDocumentStore())
            {
                new IndexWithCustomMethod().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "AAABBB" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    Assert.Single(session.Query<User, IndexWithCustomMethod>());
                }
            }
        }

        [Fact]
        public void Will_throw_on_invalid_return_type()
        {
            using (var store = GetDocumentStore())
            {
                new IndexWithInvalidReturnType().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "AAABBB" });
                    session.SaveChanges();
                }

                var errors = WaitForIndexingErrors(store, timeout: TimeSpan.FromMinutes(1));
                Assert.Single(errors);
            }
        }

        [Fact]
        public void Will_throw_on_invalid_parameter_count()
        {
            using (var store = GetDocumentStore())
            {
                Assert.Throws<IndexCompilationException>(() => new IndexWithInvalidParameterCount().Execute(store));
            }
        }

        [Fact]
        public void Can_use_arrow_function_with_static_parameter_in_index()
        {
            using (var store = GetDocumentStore())
            {
                new IndexWithArrowFunctionStaticParameter().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "AAABBB" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    Assert.Single(session.Query<User, IndexWithArrowFunctionStaticParameter>());
                }
            }
        }

        [Fact]
        public void Can_use_arrow_function_with_dynamic_parameter_in_index()
        {
            using (var store = GetDocumentStore())
            {
                new IndexWithArrowFunctionDynamicParameter().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "AAABBB" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    Assert.Single(session.Query<User, IndexWithArrowFunctionDynamicParameter>());
                }
            }
        }

        [Fact]
        public void Can_use_arrow_function_with_dynamic_and_static_parameter_in_index()
        {
            using (var store = GetDocumentStore())
            {
                new IndexWithArrowFunctionDynamicAndStaticParameter().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "AAABBB", Flag = true });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    Assert.Single(session.Query<User, IndexWithArrowFunctionDynamicAndStaticParameter>());
                }
            }
        }
    }
}
