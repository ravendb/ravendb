using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Tests.Core.Utils.Entities;
using Xunit;


namespace NewClientTests.NewClient
{
    public class lazy : RavenTestBase
    {
        [Fact]
        public void CanLazilyLoadEntity()
        {
            const string COMPANY1_ID = "companies/1";
            const string COMPANY2_ID = "companies/2";

            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    PutCommand(s, new Company {Id = COMPANY1_ID}, COMPANY1_ID);
                    PutCommand(s, new Company {Id = COMPANY2_ID}, COMPANY2_ID);
                }

                using (var session = store.OpenSession())
                {
                    Lazy<Company> lazyOrder = session.Advanced.Lazily.Load<Company>(COMPANY1_ID);
                    Assert.False(lazyOrder.IsValueCreated);
                    var order = lazyOrder.Value;
                    Assert.Equal(COMPANY1_ID, order.Id);

                    Lazy<Dictionary<string, Company>> lazyOrders = session.Advanced.Lazily.Load<Company>(new String[] { COMPANY1_ID, COMPANY2_ID });
                    Assert.False(lazyOrders.IsValueCreated);
                    var orders = lazyOrders.Value;
                    Assert.Equal(2, orders.Count);
                    Company company1;
                    Company company2;
                    orders.TryGetValue(COMPANY1_ID, out company1);
                    orders.TryGetValue(COMPANY2_ID, out company2);

                    Assert.NotNull(company1);
                    Assert.NotNull(company2);
                    Assert.Equal(COMPANY1_ID,company1.Id);
                    Assert.Equal(COMPANY2_ID, company2.Id);
                }
            }
        }

        [Fact]
        public void CanExecuteAllPendingLazyOperations()
        {
            const string COMPANY1_ID = "companies/1";
            const string COMPANY2_ID = "companies/2";

            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    PutCommand(s, new Company { Id = COMPANY1_ID }, COMPANY1_ID);
                    PutCommand(s, new Company { Id = COMPANY2_ID }, COMPANY2_ID);
                }

                using (var session = store.OpenSession())
                {
                    Company company1 = null;
                    Company company2 = null;

                    session.Advanced.Lazily.Load<Company>(COMPANY1_ID, x => company1 = x);
                    session.Advanced.Lazily.Load<Company>(COMPANY2_ID, x => company2 = x);
                    Assert.Null(company1);
                    Assert.Null(company2);

                    session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
                    Assert.NotNull(company1);
                    Assert.NotNull(company2);
                    Assert.Equal(COMPANY1_ID, company1.Id);
                    Assert.Equal(COMPANY2_ID, company2.Id);
                }
            }
        }

        [Fact]
        public void WithQueuedActions_Load()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "oren" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    User user = null;
                    session.Advanced.Lazily.Load<User>("users/1", x => user = x);
                    session.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
                    Assert.NotNull(user);
                }

            }
        }

        
    }
}
