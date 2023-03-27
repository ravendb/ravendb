using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_19548 : RavenTestBase
    {
        public RavenDB_19548(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task RawRavenQueryProjectionWithoutNewKeyword()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Omer", Id = "123456", Age = "28", Birthday = new DateTime(1994, 3, 22) });
                    await session.SaveChangesAsync();
                }

                /* should work:
                * simple linq select
                */
                using (var session = store.OpenAsyncSession())
                {
                    var asyncDocumentQuery = from result in session.Query<User>() select result.Id.Length;

                    Assert.Equal("from 'Users' select Id.Length"
                        , asyncDocumentQuery.ToString());
                }

                /* should work:
                 * primitive type with 'select new'
                 */
                using (var session = store.OpenAsyncSession())
                {
                    var asyncDocumentQuery = from result in session.Query<User>()
                        let ret = RavenQuery.Raw<int>("result.Id.length")
                        select new { userIdLength = ret };

                    Assert.Equal(
                        "declare function output(result) {\r\n\tvar ret = result.Id.length;\r\n\treturn { userIdLength : ret };\r\n}\r\nfrom 'Users' as result select output(result)"
                        , asyncDocumentQuery.ToString());
                }

                /*
                 * shouldn't work:
                 * primitive type without 'select new' (just 'select').
                 * client throws nested exception InvalidOperationException(NotSupportedException)
                 */
                using (var session = store.OpenAsyncSession())
                {
                    var asyncDocumentQuery = from result in session.Query<User>()
                        let ret = RavenQuery.Raw<int>("result.Id.length")
                        select ret;

                    Action act = () => asyncDocumentQuery.ToString();
                    var exception = Assert.Throws<NotSupportedException>(act);
                    Assert.Equal("Unsupported parameter type Int32. Primitive types/string types are not allowed in Raw<T>() method.",
                        exception.InnerException.Message);
                }

                /* should work:
                 * object type X 'select new'
                 */
                using (var session = store.OpenAsyncSession())
                {
                    var asyncDocumentQuery = from result in session.Query<User>()
                        let ret = RavenQuery.Raw<object>("result.Id.length")
                        select new { myval = ret };

                    Assert.Equal(
                        "declare function output(result) {\r\n\tvar ret = result.Id.length;\r\n\treturn { myval : ret };\r\n}\r\nfrom 'Users' as result select output(result)"
                        , asyncDocumentQuery.ToString());
                }

                /* should work:
                 * object type X 'select' with JS object inside Raw() function
                 */
                using (var session = store.OpenAsyncSession())
                {
                    var asyncDocumentQuery = from result in session.Query<User>()
                        let ret = RavenQuery.Raw<object>("{ retval : result.Id.length}")
                        select ret;

                    Assert.Equal(
                        "declare function output(result) {\r\n\tvar ret = { retval : result.Id.length};\r\n\treturn ret;\r\n}\r\nfrom 'Users' as result select output(result)"
                        , asyncDocumentQuery.ToString());
                }

                /* shouldn't work:
                 * object type X 'select'  
                 * server throws InvalidOperationException: "Query returning a single function call result must return an object"
                 */
                using (var session = store.OpenAsyncSession())
                {
                    var asyncDocumentQuery = from result in session.Query<User>()
                        let ret = RavenQuery.Raw<object>("result.Id.length")
                        select ret;

                    Assert.Equal(
                        "declare function output(result) {\r\n\tvar ret = result.Id.length;\r\n\treturn ret;\r\n}\r\nfrom 'Users' as result select output(result)"
                        , asyncDocumentQuery.ToString());

                    var exception = Assert.ThrowsAsync<Raven.Client.Exceptions.RavenException>(async () => await asyncDocumentQuery.ToListAsync());
                    Assert.StartsWith("System.InvalidOperationException: Query returning a single function call result must return an object",
                        exception.Result.Message);
                }

                /* should work:
                 * object type X 'select function' with JS object inside
                 */
                using (var session = store.OpenAsyncSession())
                {
                    var asyncDocumentQuery = from result in session.Query<User>()
                        select RavenQuery.Raw<object>("{ retval : result.Id.length }");

                    Assert.Equal(
                        "declare function output(result) {\r\n\treturn { retval : result.Id.length };\r\n}\r\nfrom 'Users' as result select output(result)"
                        , asyncDocumentQuery.ToString());
                }

                /* shouldn't work:
                 * object type X 'select function'
                 * server throws InvalidOperationException: "Query returning a single function call result must return an object"
                 */
                using (var session = store.OpenAsyncSession())
                {
                    var asyncDocumentQuery = from result in session.Query<User>()
                        select RavenQuery.Raw<object>("result.Id.length");

                    Assert.Equal(
                        "declare function output(result) {\r\n\treturn result.Id.length;\r\n}\r\nfrom 'Users' as result select output(result)"
                        , asyncDocumentQuery.ToString());

                    var exception = Assert.ThrowsAsync<Raven.Client.Exceptions.RavenException>(async () => await asyncDocumentQuery.ToListAsync());
                    Assert.StartsWith("System.InvalidOperationException: Query returning a single function call result must return an object",
                        exception.Result.Message);
                }

                /* shouldn't work:
                * primitive type X 'select function'
                * client throws nested exception InvalidOperationException(NotSupportedException)
                */
                using (var session = store.OpenAsyncSession())
                {
                    var asyncDocumentQuery = from result in session.Query<User>()
                        select RavenQuery.Raw<int>("result.Id.length");

                    Action act = () => asyncDocumentQuery.ToString();
                    var exception = Assert.Throws<NotSupportedException>(act);
                    Assert.Equal("Unsupported parameter type Int32. Primitive types/string types are not allowed in Raw<T>() method.",
                        exception.InnerException.Message);
                }


            }

        }

        private class User
        {
            public string Id { get; set; }
            public string Age { get; set; }
            public DateTime Birthday { get; set; }
            public string Name { get; set; }
        }
    }


}


