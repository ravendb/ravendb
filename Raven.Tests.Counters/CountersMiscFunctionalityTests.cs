using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Raven.Client.Counters;
using Raven.Database.Counters;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Counters
{
    public class CountersMiscFunctionalityTests : RavenBaseCountersTest
    {
        [Theory]
        [InlineData(5, 1)]
        [InlineData(1, 5)]
        [InlineData(5, 5)]
        public async Task GetCounterStorageNameAndGroups_should_handle_paging_properly(int groupCount, int counterInEachGroupCount)
        {
            using (var store = NewRemoteCountersStore(DefaultCounterStorageName))
            {
                await SetupCounters(store, groupCount, counterInEachGroupCount);
                var withoutPaging = await store.Admin.GetCounterStorageNameAndGroups();

                for (int i = 0; i < counterInEachGroupCount * groupCount; i++)
                {
                    var withPaging = await store.Admin.GetCounterStorageNameAndGroups(skip:i, take:1);
                    Assert.Equal(1,withPaging.Count);

                    var expected = withoutPaging.Skip(i).Take(1).First();
                    Assert.Equal(expected.Group,withPaging.First().Group);
                    Assert.Equal(expected.Name, withPaging.First().Name);
                }
            }
        }

        [Theory]
        [InlineData(5, 1)]
        [InlineData(1, 5)]
        [InlineData(5, 5)]
        public async Task GetCounterSummary_should_handle_paging_properly(int groupCount, int counterInEachGroupCount)
        {
            using (var store = NewRemoteCountersStore(DefaultCounterStorageName))
            {
                await SetupCounters(store, groupCount, counterInEachGroupCount);
                var withoutPaging = await store.Advanced.GetCounters();

                for (int i = 0; i < counterInEachGroupCount * groupCount; i++)
                {
                    var withPaging = await store.Advanced.GetCounters(i, 1);
                    Assert.Equal(1, withPaging.Count);

                    var expected = withoutPaging.Skip(i).Take(1).First();
                    Assert.Equal(expected.Total, withPaging.First().Total);
                    Assert.Equal(expected.CounterName, withPaging.First().CounterName);
                    Assert.Equal(expected.GroupName, withPaging.First().GroupName);
                    Assert.Equal(expected.Decrements, withPaging.First().Decrements);
                    Assert.Equal(expected.Increments, withPaging.First().Increments);
                }
            }
        }
        
        [Fact]
        public void GetCounterSummary_should_handle_skip_take_properly()
        {
            using (var counterStorage = NewCounterStorage())
            {
                SetupCounters(counterStorage);
                using (var reader = counterStorage.CreateReader())
                {
                    for (int i = 0; i < 3; i++)
                    {
                        var summaries = reader.GetCounterSummariesByGroup("ga", i, 1).ToList();
                        Assert.NotNull(summaries);
                        Assert.Equal(1, summaries.Count);
                        Assert.Equal(i, summaries[0].Total);
                    }
                }
            }
        }

        private static async Task SetupCounters(ICounterStore store, int groupCount, int countersInEachGroupCount)
        {
            const char initialSuffix = 'a';
            for(int group = 0; group < groupCount; group++)
                for (int counter = 0; counter < countersInEachGroupCount; counter++)
                {
                    var groupName = "g" + (char)(initialSuffix + group);
                    var counterName = "c" + (char)(initialSuffix + counter); 
                    await store.ChangeAsync(groupName, counterName, counter + 1);
                }
        }


        private static void SetupCounters(CounterStorage counterStorage)
        {
            using (var writer = counterStorage.CreateWriter())
            {
                writer.Store("ga", "ca", 0);
                writer.Store("ga", "cb", 1);
                writer.Store("ga", "cc", 2);
                writer.Store("gb", "cb", 2);
                writer.Store("gc", "cc", 3);
                writer.Commit();
            }
        }
    }
}
