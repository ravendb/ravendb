using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Document;

namespace FastTests.Client.Subscriptions
{
    public class SubscriptionTestBase: RavenTestBase
    {
        public class Thing
        {
            public string Name { get; set; }
        }

        protected static void CreateDocuments(DocumentStore store, int amount)
        {
            using (var session = store.OpenSession())
            {
                for (var i = 0; i < amount; i++)
                {
                    session.Store(new Thing
                    {
                        Name = $"ThingNo{i}"
                    });
                }
                session.SaveChanges();
            }
        }

        protected async Task AsyncSpin(Func<bool> action, int amount)
        {
            var sp = Stopwatch.StartNew();

            while (sp.ElapsedMilliseconds < amount && action() == false)
            {
                await Task.Delay(50).ConfigureAwait(false);
            }
        }
    }
}
