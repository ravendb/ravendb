// -----------------------------------------------------------------------
//  <copyright file="LoadDocumentWithSelectMany.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
    public class LoadDocumentWithSelectMany : RavenTest
    {
        private const int refCount = 2;

        public class User
        {
            public string Id;
        }
        public class Index : AbstractIndexCreationTask<User, Index.Result>
        {
            public class Result
            {
                public string Id;
                public int Count;
            }

            public Index()
            {
                Map = users =>
                    from user in users
                    from i in Enumerable.Range(0, refCount)
                    select new
                    {
                        Count = LoadDocument<User>("customers/" + i) == null ? 1 : 0,
                        user.Id
                    };
                Reduce = results =>
                    from result in results
                    group result by result.Id
                        into g
                        select new { Id = g.Key, Count = g.Sum(x => x.Count) };
            }
        }

        protected override void CreateDefaultIndexes(IDocumentStore documentStore)
        {
            
        }

        [Fact]
        public void ShouldRecordReferncesProperly()
        {
            using (var store = NewDocumentStore(requestedStorage:"esent"))
            {
                var count = 2;
                for (int i = 0; i < count; i++)
                {
                    using (var s = store.OpenSession())
                    {
                        s.Store(new User(), "users/" + i);
                        s.SaveChanges();
                    }
                }

                new Index().Execute(store);

                WaitForIndexing(store);

                var dic = new Dictionary<string, int>();
                for (int i = 0; i < count; i++)
                {
                    store.SystemDatabase.TransactionalStorage.Batch(accessor =>
                    {
                        dic["users/" + i] = accessor.Indexing.GetDocumentsReferencesFrom("users/" + i).Count();
                    });
                }
                foreach (var kvp in dic)
                {
                    if (kvp.Value != refCount)
                    {
                        Assert.False(true, "Value of " + kvp.Key + " is " + kvp.Value);
                    }
                }
            }
        }
    }
}
