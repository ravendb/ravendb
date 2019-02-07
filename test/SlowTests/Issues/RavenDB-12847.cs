using System.Linq;
using System.Text;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12847 : RavenTestBase
    {
        [Fact]
        public void Can_user_normalize_in_index()
        {
            CanUseOverLoad(new Test_Normalize());
        }

        [Fact]
        public void Can_use_normalize_overload_in_index()
        {
            CanUseOverLoad(new Test_Normalize_Overload());
        }

        [Fact]
        public void Can_use_is_normalized_in_index()
        {
            CanUseOverLoad(new Test_Is_Normalized());
        }

        [Fact]
        public void Can_use_is_normalized_overload()
        {
            CanUseOverLoad(new Test_Is_Normalized_Overload());
        }

        private void CanUseOverLoad(AbstractIndexCreationTask index)
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Company = "Hibernating Rhinos"
                    });
                    session.SaveChanges();
                }

                index.Execute(store);

                WaitForIndexing(store);

                var errors = store.Maintenance.Send(new GetIndexErrorsOperation());
                Assert.Equal(0, errors[0].Errors.Length);
            }
        }


        private class Test_Normalize : AbstractIndexCreationTask<Order>
        {
            public Test_Normalize()
            {
                Map = orders => from order in orders
                    select new
                    {
                        Company = order.Company.Normalize()
                    };
            }
        }

        private class Test_Normalize_Overload : AbstractIndexCreationTask<Order>
        {
            public Test_Normalize_Overload()
            {
                Map = orders => from order in orders
                    select new
                    {
                        NormalizeC = order.Company.Normalize(NormalizationForm.FormC),
                        NormalizeD = order.Company.Normalize(NormalizationForm.FormD),
                        NormalizeKC = order.Company.Normalize(NormalizationForm.FormKC),
                        NormalizeKD = order.Company.Normalize(NormalizationForm.FormKD)
                    };
            }
        }

        private class Test_Is_Normalized : AbstractIndexCreationTask<Order>
        {
            public Test_Is_Normalized()
            {
                Map = orders => from order in orders
                    select new
                    {
                        IsNormalized = order.Company.IsNormalized()
                    };
            }
        }

        private class Test_Is_Normalized_Overload : AbstractIndexCreationTask<Order>
        {
            public Test_Is_Normalized_Overload()
            {
                Map = orders => from order in orders
                    select new
                    {
                        IsNormalizedC = order.Company.IsNormalized(NormalizationForm.FormC),
                        IsNormalizedD = order.Company.IsNormalized(NormalizationForm.FormD),
                        IsNormalizedKC = order.Company.IsNormalized(NormalizationForm.FormKC),
                        IsNormalizedKD = order.Company.IsNormalized(NormalizationForm.FormKD)
                    };
            }
        }
    }
}
