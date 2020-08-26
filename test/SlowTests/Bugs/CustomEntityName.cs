//-----------------------------------------------------------------------
// <copyright file="CustomEntityName.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Util;
using Sparrow.Server;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class CustomEntityName : RavenTestBase
    {
        public CustomEntityName(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanCustomizeEntityName()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.FindCollectionName = ReflectionUtil.GetFullNameWithoutVersionInformation
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Foo { Name = "Ayende" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var typeName = ReflectionUtil.GetFullNameWithoutVersionInformation(typeof(Foo));

                    var all = session
                        .Advanced
                        .DocumentQuery<Foo>(collectionName: typeName)
                        .WaitForNonStaleResults(TimeSpan.FromMilliseconds(1000))
                        .ToList();
                    Assert.Equal(1, all.Count);
                }

            }
        }

        public class Foo
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        #region FindCollectionName special chars
        private class User 
        {
            public string Id { get; set; }
            public string CarId { get; set; }
        }
        private class Car 
        {
            public string Id { get; set; }
            public string Manufacturer { get; set; }
        }

        private class IndexResult
        {
            public string CarManufacturer { get; set; }
        }
        
        private class LoadWithStringIndex : AbstractIndexCreationTask<User, IndexResult>
        {
            public LoadWithStringIndex()
            {
                Map = users =>
                    users.Select(user => new {CarManufacturer = LoadDocument<Car>(user.CarId.ToString()).Manufacturer});

            }
        }
        private class LoadWithLazyStringIndex : AbstractIndexCreationTask<User, IndexResult>
        {
            public LoadWithLazyStringIndex()
            {
                Map = users =>
                    from user in users
                    select new { CarManufacturer = LoadDocument<Car>(user.CarId.ToString()).Manufacturer};
            }
        }

        public static IEnumerable<object[]> GetCharactersToTest()
        {
            return Enumerable.Range(1, 31)
                .Select(i => (char)i)
                .Concat(new[] { 'a', '-', '\'', '\"', '\\', '\a', '\b', '\f', '\n', '\r', '\t', '\v' })
                .Distinct()
                .Select(c => new object[]{c});
        }

        
        [Theory]
        [MemberData(nameof(GetCharactersToTest))]
        public async Task FindCollectionName_WhenIndexWithLoadByString(char c)
        {
            await TestWhenCollectionAndIdContainSpecialChars<LoadWithStringIndex>(c);
        }
        
        [Theory]
        [MemberData(nameof(GetCharactersToTest))]
        public async Task FindCollectionName_WhenIndexWithLoadByLazyString(char c)
        {
            await TestWhenCollectionAndIdContainSpecialChars<LoadWithLazyStringIndex>(c);
        }

        private async Task TestWhenCollectionAndIdContainSpecialChars<T>(char c) where T : AbstractGenericIndexCreationTask<IndexResult>, new()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.FindCollectionName = type => "Test" + c + DocumentConventions.DefaultGetCollectionName(type)
            });

            using (var session = store.OpenAsyncSession())
            {
                var car = new Car {Manufacturer = "BMW"};
                await session.StoreAsync(car);
                await session.StoreAsync(new User {CarId = car.Id});
                await session.SaveChangesAsync();
            }

            var index = new T();
            await index.ExecuteAsync(store);
            WaitForIndexing(store);

            using (var session = store.OpenAsyncSession())
            {
                var results = await session
                    .Query<IndexResult, T>()
                    .Where(x => x.CarManufacturer == "BMW")
                    .OfType<User>()
                    .ToArrayAsync();

                Assert.Equal(1, results.Length);
            }
        }
        
        [Theory]
        [MemberData(nameof(GetCharactersToTest))]
        public async Task FindCollectionName_WhenSubscribeToApiChanges(char c)
        {
            var mre = new AsyncManualResetEvent();
            
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.FindCollectionName = type => "Test" + c + DocumentConventions.DefaultGetCollectionName(type)
            });

            var subscription = store.Changes();
            await subscription.EnsureConnectedNow();
            var observableWithTask = subscription
                .ForDocumentsInCollection<User>();
            observableWithTask.Subscribe(change => mre.Set());
            await observableWithTask.EnsureSubscribedNow();
            
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User());
                await session.SaveChangesAsync();
            }

            Assert.True(await mre.WaitAsync(TimeSpan.FromSeconds(15)));
        }
        
        private class MultiMapIndex : AbstractMultiMapIndexCreationTask<IndexResult>
        {
            public MultiMapIndex()
            {
                AddMap<User>(users =>
                    users.Select(user => new {CarManufacturer = LoadDocument<Car>(user.CarId.ToString()).Manufacturer}));
            }
        }
        
        [Theory]
        [MemberData(nameof(GetCharactersToTest))]
        public async Task FindCollectionName_WhenIndexWithMultiMap(char c)
        {
            await TestWhenCollectionAndIdContainSpecialChars<MultiMapIndex>(c);
        }
        
        [Theory]
        [MemberData(nameof(GetCharactersToTest))]
        public async Task FindCollectionName_WhenQuery(char c)
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.FindCollectionName = type => "Test" + c + DocumentConventions.DefaultGetCollectionName(type)
            });

            using (var session = store.OpenAsyncSession())
            {
                var car = new Car {Manufacturer = "BMW"};
                await session.StoreAsync(car);
                await session.StoreAsync(new User {CarId = car.Id});
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var results = await session
                    .Query<User>()
                    .ToArrayAsync();

                Assert.Equal(1, results.Length);
            }
            
            using (var session = store.OpenAsyncSession())
            {
                var results = await session.Advanced
                    .AsyncDocumentQuery<User>()
                    .ToArrayAsync();

                Assert.Equal(1, results.Length);
            }
            
            using (var session = store.OpenSession())
            {
                var results = session
                    .Query<User>()
                    .ToArray();

                Assert.Equal(1, results.Length);
            }
            
            using (var session = store.OpenSession())
            {
                var results = session.Advanced
                    .DocumentQuery<User>()
                    .ToArray();

                Assert.Equal(1, results.Length);
            }
        }

        [Theory]
        [MemberData(nameof(GetCharactersToTest))]
        public async Task FindCollectionName_WhenLoadWithInclude(char c)
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.FindCollectionName = type => "Test" + c + DocumentConventions.DefaultGetCollectionName(type)
            });

            var user = new User();
            using (var session = store.OpenAsyncSession())
            {
                var car = new Car {Manufacturer = "BMW"};
                await session.StoreAsync(car);
                user.CarId = car.Id;
                await session.StoreAsync(user);
                await session.SaveChangesAsync();
            }
            
            using (var session = store.OpenAsyncSession())
            {
                var loadedUser = await session.Include<User, Car>(x => x.CarId)
                    .LoadAsync(user.Id);

                Assert.NotNull(loadedUser);
                Assert.True(session.Advanced.IsLoaded(user.CarId), "Included data should be loaded");
            }
        }


        [Theory]
        [MemberData(nameof(GetCharactersToTest))]
        public async Task FindCollectionName_WhenSubscribeWithInclude(char c)
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.FindCollectionName = type => "Test" + c + DocumentConventions.DefaultGetCollectionName(type)
            });

            var user = new User();
            using (var session = store.OpenAsyncSession())
            {
                var car = new Car {Manufacturer = "BMW"};
                await session.StoreAsync(car);
                user.CarId = car.Id;
                await session.StoreAsync(user);
                await session.SaveChangesAsync();
            }
            
            var name = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions<User>
            {
                Includes = builder => builder
                    .IncludeDocuments(x => x.CarId)
            });

            await using (var sub = store.Subscriptions.GetSubscriptionWorker<User>(name))
            {
                var mre = new AsyncManualResetEvent();
                var r = sub.Run(batch =>
                {
                    Assert.NotEmpty(batch.Items);
                    using (var s = batch.OpenSession())
                    {
                        foreach (var item in batch.Items)
                        {
                            s.Load<Car>(item.Result.CarId);
                        }
                        Assert.Equal(0, s.Advanced.NumberOfRequests);
                    }
                    mre.Set();
                });
                var isSet = await mre.WaitAsync(TimeSpan.FromSeconds(30));

                await sub.DisposeAsync();
                await r;// no error
                Assert.True(isSet);
            }
        }
        #endregion    
    }
}
