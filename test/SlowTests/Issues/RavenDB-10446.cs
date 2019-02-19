using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10446 : RavenTestBase        
    {
        public enum Colors
        {
            Red,
            Green,
            Brown
        }

        public class Shirt
        {
            public Colors Color { get; set; }
            public string ModelName { get; set; }
            public double Price { get; set; }
            public string ShirtType { get; set; }
            public DateTime ManufactureDate { get; set; }
        }

        public class Tunica
        {
        }

        public class Undershirt
        {
        }

        public class Leotard
        {

        }

        public class ValWithString
        {
            public string ColorsString;
        }


        [Fact]
        public async Task SubscriptionTypedCreationOptionsShouldSupportConstantValuesWithIndirectPath()
        {
            using (var store = GetDocumentStore())
            {
                await TestWrappedValues(store);
            }
        }

        [Fact]
        public async Task SubscriptionTypedCreationOptionsShouldSupportConstantValuesWithIndirectPathAndSaveEnumsAsIntegers()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = x => x.Conventions.SaveEnumsAsIntegers = true
            }))
            {
                await TestWrappedValues(store);
            }
        }

        private static async Task TestWrappedValues(DocumentStore store)
        {
            var tunicaType = typeof(Tunica);
            var undershirtType = typeof(Undershirt);
            var shirtWithValue = new Shirt
            {
                ManufactureDate = DateTime.Now,
                Price = 10.99,
                Color = Colors.Brown
            };

            var subsId = store.Subscriptions.Create(new Raven.Client.Documents.Subscriptions.SubscriptionCreationOptions<Shirt>
            {
                Filter = x => x.ShirtType == tunicaType.FullName && x.Color == shirtWithValue.Color && x.ManufactureDate >= shirtWithValue.ManufactureDate && x.Price > shirtWithValue.Price
            });

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Shirt
                {
                    Color = Colors.Brown,
                    ModelName = "Brown Tunica No 5",
                    ShirtType = tunicaType.FullName,
                    Price = 99.9,
                    ManufactureDate = DateTime.Now.AddDays(1)
                });

                await session.StoreAsync(new Shirt
                {
                    Color = Colors.Green,
                    ModelName = "Navy Undershirt No 3",
                    ShirtType = undershirtType.FullName,
                    Price = 55.3,
                    ManufactureDate = DateTime.Now.AddDays(1)
                });

                await session.SaveChangesAsync();
            }

            var subsWorker = store.Subscriptions.GetSubscriptionWorker<Shirt>(new Raven.Client.Documents.Subscriptions.SubscriptionWorkerOptions(subsId)
            {
                CloseWhenNoDocsLeft = true,
                TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
            });

            var shirts = new List<string>();
            try
            {
                await subsWorker.Run(batch => batch.Items.ForEach(i => shirts.Add(i.Result.ModelName)));
            }
            catch (Exception) { }

            Assert.Equal(1, shirts.Count);
            Assert.Equal("Brown Tunica No 5", shirts[0]);
        }
    }
}
