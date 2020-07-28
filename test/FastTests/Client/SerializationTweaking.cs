using System;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class SerializationTweaking : RavenTestBase
    {
        public class ClassWithByRef
        {
            private int _x;
            public ref int X => ref _x;

            public int Y { get; set; }
        }

        public unsafe class ClassWithPtr
        {
            public int* X { get; set; }
            public int Y { get; set; }
        }

        public SerializationTweaking(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Should_throw_error_by_default_on_by_pointer_fields()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new ClassWithPtr());
                    Assert.Throws<NotSupportedException>(() => session.SaveChanges());
                }
            }
        }

        [Fact]
        public void Should_throw_error_by_default_on_by_ref_fields()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new ClassWithByRef());
                    Assert.Throws<NotSupportedException>(() => session.SaveChanges());
                }
            }
        }

        [Fact]
        public void Should_be_able_to_ignore_ref_fields()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = store =>
                {
                    store.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                    {
                        IgnoreByRefMembers = true
                    };
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new ClassWithByRef { Y = 123 }, "foo/bar");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var fetched = session.Load<ClassWithByRef>("foo/bar");
                    Assert.NotNull(fetched); //sanity check
                    Assert.Equal(123, fetched.Y);
                }
            }
        }

        [Fact]
        public void Should_be_able_to_ignore_pointer_fields()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = store =>
                {
                    store.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                    {
                        IgnoreUnsafeMembers = true
                    };
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new ClassWithPtr { Y = 123 }, "foo/bar");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var fetched = session.Load<ClassWithPtr>("foo/bar");
                    Assert.NotNull(fetched); //sanity check
                    Assert.Equal(123, fetched.Y);
                }
            }
        }
    }
}
