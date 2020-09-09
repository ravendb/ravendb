using System;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RDBC_204 : RavenTestBase
    {
        public RDBC_204(ITestOutputHelper output) : base(output)
        {
        }

        private const string _docId = "users/1-A";

        private class User
        {
            public byte Byte { get; set; }
            public short Short { get; set; }
            public int Int { get; set; }
            public uint Uint { get; set; }
            public long Long { get; set; }
            public ulong Ulong { get; set; }
            public Guid Guid { get; set; }
            public decimal Decimal { get; set; }
            public float Float { get; set; }
            public double Double { get; set; }
            public bool Bool { get; set; }
            public string String { get; set; }
        }

        [Fact]
        public void CanPatchWithByteProperty()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Byte = 1
                    }, _docId);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // explicitly specify id & type
                    session.Advanced.Patch<User, byte>(_docId, u => u.Byte, 123);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(123, loaded.Byte);

                    // infer type & the id from entity
                    session.Advanced.Patch(loaded, u => u.Byte, 234);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(234, loaded.Byte);
                }
            }
        }

        [Fact]
        public void CanPatchWithShortProperty()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Short = 1
                    }, _docId);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // explicitly specify id & type
                    session.Advanced.Patch<User, short>(_docId, u => u.Short, 123);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(123, loaded.Short);

                    // infer type & the id from entity
                    session.Advanced.Patch(loaded, u => u.Short, 234);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(234, loaded.Short);
                }
            }
        }

        [Fact]
        public void CanPatchWithIntProperty()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Int = 1
                    }, _docId);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // explicitly specify id & type
                    session.Advanced.Patch<User, int>(_docId, u => u.Int, 123);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(123, loaded.Int);

                    // infer type & the id from entity
                    session.Advanced.Patch(loaded, u => u.Int, 234);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(234, loaded.Int);
                }
            }
        }

        [Fact]
        public void CanPatchWithUIntProperty()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Uint = 1
                    }, _docId);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // explicitly specify id & type
                    session.Advanced.Patch<User, uint>(_docId, u => u.Uint, 123);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal((uint)123, loaded.Uint);

                    session.Advanced.Patch<User, uint>(loaded, u => u.Uint, 234);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal((uint)234, loaded.Uint);
                }
            }
        }

        [Fact]
        public void CanPatchWithLongProperty()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Long = 1
                    }, _docId);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // explicitly specify id & type
                    session.Advanced.Patch<User, long>(_docId, u => u.Long, 123);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(123, loaded.Long);

                    // infer type & the id from entity
                    session.Advanced.Patch(loaded, u => u.Long, 234);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(234, loaded.Long);
                }
            }
        }

        [Fact]
        public void CanPatchWithULongProperty()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Ulong = 1
                    }, _docId);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // explicitly specify id & type
                    session.Advanced.Patch<User, ulong>(_docId, u => u.Ulong, 123);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal((ulong)123, loaded.Ulong);

                    session.Advanced.Patch<User, ulong>(loaded, u => u.Ulong, 234);
                    SaveChangesWithTryCatch(session, loaded);
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal((ulong)234, loaded.Ulong);
                }
            }
        }

        [Fact]
        public void CanPatchWithGuidProperty()
        {
            byte[] bytes = {0, 1, 2, 3, 4, 5, 6, 7,8, 9, 10, 11, 12, 13, 14, 15};
            Guid guid = new Guid(bytes);

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Guid = guid
                    }, _docId);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    bytes[0] = 123;
                    guid = new Guid(bytes);
                    // explicitly specify id & type
                    session.Advanced.Patch<User, Guid>(_docId, u => u.Guid, guid);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(guid, loaded.Guid);

                    bytes[1] = 234;
                    guid = new Guid(bytes);
                    // infer type & the id from entity
                    session.Advanced.Patch(loaded, u => u.Guid, guid);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(guid, loaded.Guid);
                }
            }
        }

        [Fact]
        public void CanPatchWithDecimalProperty()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Decimal = (decimal)1.0
                    }, _docId);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // explicitly specify id & type
                    session.Advanced.Patch<User, decimal>(_docId, u => u.Decimal, (decimal)123.4);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal((decimal)123.4, loaded.Decimal);

                    // infer type & the id from entity
                    session.Advanced.Patch(loaded, u => u.Decimal, (decimal)234.56);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal((decimal)234.56, loaded.Decimal);
                }
            }
        }

        [Fact]
        public void CanPatchWithFloatProperty()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Float = (float)1.0
                    }, _docId);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // explicitly specify id & type
                    session.Advanced.Patch<User, float>(_docId, u => u.Float, (float)123.4);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal((float)123.4, loaded.Float);

                    // infer type & the id from entity
                    session.Advanced.Patch(loaded, u => u.Float, (float)234.56);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal((float)234.56, loaded.Float);
                }
            }
        }

        [Fact]
        public void CanPatchWithDoubleProperty()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Double = 1.0
                    }, _docId);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // explicitly specify id & type
                    session.Advanced.Patch<User, double>(_docId, u => u.Double, 123.4);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(123.4, loaded.Double);

                    // infer type & the id from entity
                    session.Advanced.Patch(loaded, u => u.Double, 234.56);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal(234.56, loaded.Double);
                }
            }
        }

        [Fact]
        public void CanPatchWithBoolProperty()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Bool = false
                    }, _docId);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // explicitly specify id & type
                    session.Advanced.Patch<User, bool>(_docId, u => u.Bool, true);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.True(loaded.Bool);

                    // infer type & the id from entity
                    session.Advanced.Patch(loaded, u => u.Bool, false);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.False(loaded.Bool);
                }
            }
        }

        [Fact]
        public void CanPatchWithStringProperty()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        String = "1"
                    }, _docId);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    // explicitly specify id & type
                    session.Advanced.Patch<User, string>(_docId, u => u.String, "123");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal("123", loaded.String);

                    // infer type & the id from entity
                    session.Advanced.Patch(loaded, u => u.String, "234");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loaded = session.Load<User>(_docId);
                    Assert.Equal("234", loaded.String);
                }
            }
        }

    }
}
