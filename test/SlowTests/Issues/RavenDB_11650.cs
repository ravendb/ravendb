using System;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Text;
using FastTests;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11650 : RavenTestBase
    {
        
        [Fact]
        public void CanLimitOffsetCollectionQuery()
        {
            using (var store = GetDocumentStore())
            {
                InsertUsers(store);
                
                using (var session = store.OpenSession())
                {
                    // without offset 
                    {
                        // try to get all users
                        var result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users", 0, Int32.MaxValue));
                        VerifyDocuments(result, 101, 300, null);
                    
                        // try to get with RQL limit
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users limit 20", 0, Int32.MaxValue));
                        VerifyDocuments(result, 101, 120, 20);
                    
                        // try to get with URL limit
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users", 0, 20));
                        VerifyDocuments(result, 101, 120, null);
                    
                        // try to get with both limits RQL > URL
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users limit 20", 0, 10));
                        VerifyDocuments(result, 101, 110, 20);
                    
                        // try to get with both limits RQL < URL
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users limit 5", 0, 10));
                        VerifyDocuments(result, 101, 105, 5);
                    }
                    
                    // with URL offset 
                    {
                        // try to get all users
                        var result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users", 10, Int32.MaxValue));
                        VerifyDocuments(result, 111, 300, null);
                    
                        // try to get with RQL limit
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users limit 20", 20, Int32.MaxValue));
                        VerifyDocuments(result, 121, 140, 20);
                    
                        // try to get with URL limit
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users", 30, 20));
                        VerifyDocuments(result, 131, 150, null);
                    
                        // try to get with both limits RQL > URL
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users limit 20", 40, 10));
                        VerifyDocuments(result, 141, 150, 20);
                    
                        // try to get with both limits RQL < URL
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users limit 5", 50, 10));
                        VerifyDocuments(result, 151, 155, 5);
                    }
                    
                    // with RQL offset 
                    {
                        // try to get all users
                        var result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users offset 10", 0, Int32.MaxValue));
                        VerifyDocuments(result, 111, 300, 190);
                    
                        // try to get with RQL limit
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users limit 20, 20", 0, Int32.MaxValue));
                        VerifyDocuments(result, 121, 140, 20);
                    
                        // try to get with URL limit
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users offset 30", 0, 20));
                        VerifyDocuments(result, 131, 150, 170);
                    
                        // try to get with both limits RQL > URL
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users limit 40, 20", 0, 10));
                        VerifyDocuments(result, 141, 150, 20);
                    
                        // try to get with both limits RQL < URL
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users limit 50, 5", 0, 10));
                        VerifyDocuments(result, 151, 155, 5);
                    }
                    
                    // with RQL & URL offset 
                    {
                        // try to get all users
                        var result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users offset 10", 1, Int32.MaxValue));
                        VerifyDocuments(result, 112, 300, 190);
                    
                        // try to get with RQL limit
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users limit 20, 20", 2, Int32.MaxValue));
                        VerifyDocuments(result, 123, 142, 20);
                    
                        // try to get with URL limit
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users offset 30", 3, 20));
                        VerifyDocuments(result, 134, 153, 170);
                    
                        // try to get with both limits RQL > URL
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users limit 40, 20", 4, 10));
                        VerifyDocuments(result, 145, 154, 20);
                    
                        // try to get with both limits RQL < URL
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users limit 50, 5", 5, 10));
                        VerifyDocuments(result, 156, 160, 5);
                    }
                    
                    // capping to query limit
                    {
                        // try to get all users
                        var result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users limit 1000", 0, Int32.MaxValue));
                        VerifyDocuments(result, 101, 300, 200);
                        
                        // huge URL limit
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users limit 50", 0, 1000));
                        VerifyDocuments(result, 101, 150, 50);
                        
                        // huge RQL limit
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users limit 1000", 0, 50));
                        VerifyDocuments(result, 101, 150, 200);
                        
                        // huge URL & RQL limits
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users limit 1000", 0, 2000));
                        VerifyDocuments(result, 101, 300, 200);
                        
                        // try to get all users with offset
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users limit 1000", 10, Int32.MaxValue));
                        VerifyDocuments(result, 111, 300, 200);
                        
                        // huge URL limit with offset
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users limit 50", 10, 1000));
                        VerifyDocuments(result, 111, 160, 50);
                        
                        // huge RQL limit with offset
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users limit 1000", 10, 50));
                        VerifyDocuments(result, 111, 160, 200);
                        
                        // huge URL & RQL limits with offset
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, "from Users limit 1000", 10, 2000));
                        VerifyDocuments(result, 111, 300, 200);
                    }
                }
            }
        }
        
        [Fact]
        public void CanLimitOffsetStartsWithQuery()
        {
            using (var store = GetDocumentStore())
            {
                InsertUsers(store);
                
                using (var session = store.OpenSession())
                {
                    // without offset 
                    {
                        // try to get all users
                        var result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users where (startsWith(id(),""users/"")) ", 0, Int32.MaxValue));
                        VerifyDocuments(result, 101, 300, null);
                    
                        // try to get with RQL limit
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users where (startsWith(id(),""users/"")) limit 20", 0, Int32.MaxValue));
                        VerifyDocuments(result, 101, 120, 20);
                    
                        // try to get with URL limit
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users where (startsWith(id(),""users/""))", 0, 20));
                        VerifyDocuments(result, 101, 120, null);
                    
                        // try to get with both limits RQL > URL
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users where (startsWith(id(),""users/"")) limit 20", 0, 10));
                        VerifyDocuments(result, 101, 110, 20);
                    
                        // try to get with both limits RQL < URL
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users where (startsWith(id(),""users/"")) limit 5", 0, 10));
                        VerifyDocuments(result, 101, 105, 5);
                    }
                    
                    // with URL offset 
                    {
                        // try to get all users
                        var result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users where (startsWith(id(),""users/""))", 10, Int32.MaxValue));
                        VerifyDocuments(result, 111, 300, null);
                    
                        // try to get with RQL limit
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users where (startsWith(id(),""users/"")) limit 20", 20, Int32.MaxValue));
                        VerifyDocuments(result, 121, 140, 20);
                    
                        // try to get with URL limit
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users where (startsWith(id(),""users/""))", 30, 20));
                        VerifyDocuments(result, 131, 150, null);
                    
                        // try to get with both limits RQL > URL
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users where (startsWith(id(),""users/"")) limit 20", 40, 10));
                        VerifyDocuments(result, 141, 150, 20);
                    
                        // try to get with both limits RQL < URL
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users where (startsWith(id(),""users/"")) limit 5", 50, 10));
                        VerifyDocuments(result, 151, 155, 5);
                    }
                    
                    // with RQL offset 
                    {
                        // try to get all users
                        var result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users where (startsWith(id(),""users/"")) offset 10", 0, Int32.MaxValue));
                        VerifyDocuments(result, 111, 300, -1);
                    
                        // try to get with RQL limit
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users where (startsWith(id(),""users/"")) limit 20, 20", 0, Int32.MaxValue));
                        VerifyDocuments(result, 121, 140, 20);
                    
                        // try to get with URL limit
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users where (startsWith(id(),""users/"")) offset 30", 0, 20));
                        VerifyDocuments(result, 131, 150, -1);
                    
                        // try to get with both limits RQL > URL
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users where (startsWith(id(),""users/"")) limit 40, 20", 0, 10));
                        VerifyDocuments(result, 141, 150, 20);
                    
                        // try to get with both limits RQL < URL
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users where (startsWith(id(),""users/"")) limit 50, 5", 0, 10));
                        VerifyDocuments(result, 151, 155, 5);
                    }
                    
                    // with RQL & URL offset 
                    {
                        // try to get all users
                        var result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users where (startsWith(id(),""users/"")) offset 10", 1, Int32.MaxValue));
                        VerifyDocuments(result, 112, 300, -1);
                    
                        // try to get with RQL limit
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users where (startsWith(id(),""users/"")) limit 20, 20", 2, Int32.MaxValue));
                        VerifyDocuments(result, 123, 142, 20);
                    
                        // try to get with URL limit
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users where (startsWith(id(),""users/"")) offset 30", 3, 20));
                        VerifyDocuments(result, 134, 153, -1);
                    
                        // try to get with both limits RQL > URL
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users where (startsWith(id(),""users/"")) limit 40, 20", 4, 10));
                        VerifyDocuments(result, 145, 154, 20);
                    
                        // try to get with both limits RQL < URL
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users where (startsWith(id(),""users/"")) limit 50, 5", 5, 10));
                        VerifyDocuments(result, 156, 160, 5);
                    }
                    
                    // capping to query limit
                    {
                        // try to get all users
                        var result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users limit 1000", 0, Int32.MaxValue));
                        VerifyDocuments(result, 101, 300, 200);
                        
                        // huge URL limit
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users limit 50", 0, 1000));
                        VerifyDocuments(result, 101, 150, 50);
                        
                        // huge RQL limit
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users limit 1000", 0, 50));
                        VerifyDocuments(result, 101, 150, 200);
                        
                        // huge URL & RQL limits
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users limit 1000", 0, 2000));
                        VerifyDocuments(result, 101, 300, 200);
                        
                        // try to get all users with offset
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users limit 1000", 10, Int32.MaxValue));
                        VerifyDocuments(result, 111, 300, 200);
                        
                        // huge URL limit with offset
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users limit 50", 10, 1000));
                        VerifyDocuments(result, 111, 160, 50);
                        
                        // huge RQL limit with offset
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users limit 1000", 10, 50));
                        VerifyDocuments(result, 111, 160, 200);
                        
                        // huge URL & RQL limits with offset
                        result = store.Maintenance.Send(new QueryWithUrlPagingOperation(session, @"from Users limit 1000", 10, 2000));
                        VerifyDocuments(result, 111, 300, 200);
                    }
                }
            }
        }

        private void VerifyDocuments(QueryResult result, int startInclusive, int lastIdInclusive, int? cappedMaxResults)
        {
            var expectedCount = lastIdInclusive - startInclusive + 1;
            Assert.Equal(expectedCount, result.Results.Length);
            Assert.Equal(cappedMaxResults, result.CappedMaxResults);

            if (expectedCount > 0)
            {
                var firstObjectId = ((BlittableJsonReaderObject)((BlittableJsonReaderObject)result.Results[0])["@metadata"])["@id"] as LazyStringValue;
                Assert.Equal("users/" + startInclusive, firstObjectId.ToString());
            
                var lastObjectId = ((BlittableJsonReaderObject)((BlittableJsonReaderObject)result.Results[result.Results.Length - 1])["@metadata"])["@id"] as LazyStringValue;
                Assert.Equal("users/" + lastIdInclusive, lastObjectId.ToString());    
            }
        }


        private void InsertUsers(DocumentStore store)
        {
            using (var bulk = store.BulkInsert())
            {
                for (var i = 100; i < 300; i++)
                {
                    bulk.Store(new User
                    {
                        Id = "users/" + (i + 1)
                    });
                }
            }
        }
        
        // This class simulates queries with start & pageSize (which are send by the studio)
        internal class QueryWithUrlPagingOperation : IMaintenanceOperation<QueryResult>
        {
            private readonly IDocumentSession _session;
            private readonly string _query;
            private readonly int _start;
            private readonly int _pageSize;

            public QueryWithUrlPagingOperation(IDocumentSession session, string query, int start, int pageSize)
            {
                _session = session;
                _query = query;
                _start = start;
                _pageSize = pageSize;
            }
            
            public RavenCommand<QueryResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new QueryWithUrlPagingCommand(_session, _query, _start, _pageSize);
            }
            
            private class QueryWithUrlPagingCommand :  RavenCommand<QueryResult>
            {

                private readonly string _query;
                private readonly int _start;
                private readonly int _pageSize;
            
                public QueryWithUrlPagingCommand(IDocumentSession session, string query, int start, int pageSize)
                {
                    _query = query;
                    _start = start;
                    _pageSize = pageSize;
                }
    
                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    var path = new StringBuilder(node.Url)
                        .Append("/databases/")
                        .Append(node.Database)
                        .Append("/queries?")
                        .Append("query=")
                        .Append(Uri.EscapeDataString(_query))
                        .Append("&start=")
                        .Append(_start.ToInvariantString())
                        .Append("&pageSize=")
                        .Append(_pageSize.ToInvariantString());

                    url = path.ToString();
                    
                    return new HttpRequestMessage
                    {
                        Method = HttpMethod.Get
                    };
                }
                
                public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
                {
                    if (response == null)
                    {
                        Result = null;
                        return;
                    }
                    Result = JsonDeserializationClient.QueryResult(response);
                }

                public override bool IsReadRequest => true;
            }
        }
    }
}
