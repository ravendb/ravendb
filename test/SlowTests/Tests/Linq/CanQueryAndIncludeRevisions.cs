using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using SlowTests.MailingList;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Linq
{
    public class CanQueryAndIncludeRevisions : RavenTestBase
    {
        public CanQueryAndIncludeRevisions(ITestOutputHelper output) : base(output)
        {
        }
        
        [Fact]
        public async Task CanQueryAndIncludeRevisionsTest()
        {
            using (var store = GetDocumentStore())
            {
                var id = "users/1";
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                        {
                            Name = "Hibernating",

                        },
                        id);
                    await session.SaveChangesAsync();
                }
                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<Company>(id);
                        user.Name = "Omer " + i;
                        await session.SaveChangesAsync();
                    }
                }
                
                WaitForUserToContinueTheTest(store);            
                using (var session = store.OpenAsyncSession())
                {
                    // var dq = session.Advanced.AsyncDocumentQuery<User>()
                    //     .
                    //     .WhereGreaterThan("Freight", 8)
                    //     .ToQueryable();
                }   
            }
        }
        
        // [Fact]
        // public void StreamDocumentQueryWithInclude()
        // {
        //     var store = GetDocumentStore();
        //     Setup(store);
        //     WaitForIndexing(store);
        //     using (var session = store.OpenSession())
        //     {
        //         var query = session.Advanced.DocumentQuery<DocumentQueryIncludeAndStreamTest.ProcessStep, DocumentQueryIncludeAndStreamTest.ProcessStepIndex>();
        //         query.WhereEquals("Group", 2);
        //         query.WhereEquals("LatestExecution", true);
        //         query.Include(p => p.StepExecutionsId);
        //         var notSupportedException = Assert.Throws<RavenException>(() =>
        //         {
        //             using (var stream = session.Advanced.Stream(query))
        //             {
        //                 while (stream.MoveNext())
        //                 {
        //
        //                 }
        //             }
        //         }).InnerException;
        //         Assert.Contains("Includes are not supported by this type of query", notSupportedException.Message);
        //     }
        // }
        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }

            public string Company { get; set; }
        }

        public class Company
        {
            public decimal AccountsReceivable { get; set; }
            public string Id { get; set; }
            public string Name { get; set; }
            public string Desc { get; set; }
            public string Email { get; set; }
            public string Address1 { get; set; }
            public string Address2 { get; set; }
            public string Address3 { get; set; }
            public List<Contact> Contacts { get; set; }
            public int Phone { get; set; }
            public CompanyType Type { get; set; }
            public List<string> EmployeesIds { get; set; }

            public enum CompanyType
            {
                Public,
                Private
            }
        }
        public class Contact
        {
            public string Id { get; set; }
            public string FirstName { get; set; }
            public string Surname { get; set; }
            public string Email { get; set; }
        }

        public class Employee
        {
            public string Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public string ReportsTo { get; set; }
        }
    }
}
