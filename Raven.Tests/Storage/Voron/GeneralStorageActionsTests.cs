using System.Linq;

using Raven.Tests.Common;

namespace Raven.Tests.Storage.Voron
{
	using Xunit;
	using Xunit.Extensions;

	[Trait("VoronTest", "StorageActionsTests")]
	public class GeneralStorageActionsTests : TransactionalStorageTestBase
    {
		[Theory]
		[PropertyData("Storages")]
		public void General_GetIdentities(string requestedStorage)
		{
			using (var storage = NewTransactionalStorage(requestedStorage))
			{
				storage.Batch(accessor =>
				{
					long totalCount;
					Assert.Empty(accessor.General.GetIdentities(0, 100, out totalCount));
					Assert.Equal(0, totalCount);
				});

				storage.Batch(accessor => accessor.General.SetIdentityValue("name1", 10));
				storage.Batch(accessor => accessor.General.SetIdentityValue("name2", 11));
				storage.Batch(accessor => accessor.General.SetIdentityValue("name3", 12));
				storage.Batch(accessor => accessor.General.SetIdentityValue("name4", 13));
				storage.Batch(accessor => accessor.General.SetIdentityValue("name5", 14));
				storage.Batch(accessor => accessor.General.SetIdentityValue("name6", 15));
				storage.Batch(accessor => accessor.General.SetIdentityValue("name7", 16));
				storage.Batch(accessor => accessor.General.SetIdentityValue("name7", 17));

				storage.Batch(accessor =>
				{
					long totalCount;

					var identities = accessor
						.General
						.GetIdentities(0, 100, out totalCount)
						.ToList();

					Assert.Equal(7, identities.Count);
					Assert.Equal(7, totalCount);
					Assert.Equal(10, identities.First(x => x.Key == "name1").Value);
					Assert.Equal(11, identities.First(x => x.Key == "name2").Value);
					Assert.Equal(12, identities.First(x => x.Key == "name3").Value);
					Assert.Equal(13, identities.First(x => x.Key == "name4").Value);
					Assert.Equal(14, identities.First(x => x.Key == "name5").Value);
					Assert.Equal(15, identities.First(x => x.Key == "name6").Value);
					Assert.Equal(17, identities.First(x => x.Key == "name7").Value);

					identities = accessor
						.General
						.GetIdentities(0, 2, out totalCount)
						.ToList();

					Assert.Equal(2, identities.Count);
					Assert.Equal(7, totalCount);
					Assert.Equal(10, identities.First(x => x.Key == "name1").Value);
					Assert.Equal(11, identities.First(x => x.Key == "name2").Value);

					identities = accessor
						.General
						.GetIdentities(1, 2, out totalCount)
						.ToList();

					Assert.Equal(2, identities.Count);
					Assert.Equal(7, totalCount);
					Assert.Equal(11, identities.First(x => x.Key == "name2").Value);
					Assert.Equal(12, identities.First(x => x.Key == "name3").Value);

					identities = accessor
						.General
						.GetIdentities(2, 3, out totalCount)
						.ToList();

					Assert.Equal(3, identities.Count);
					Assert.Equal(7, totalCount);
					Assert.Equal(12, identities.First(x => x.Key == "name3").Value);
					Assert.Equal(13, identities.First(x => x.Key == "name4").Value);
					Assert.Equal(14, identities.First(x => x.Key == "name5").Value);

					identities = accessor
						.General
						.GetIdentities(5, 3, out totalCount)
						.ToList();

					Assert.Equal(2, identities.Count);
					Assert.Equal(7, totalCount);
					Assert.Equal(15, identities.First(x => x.Key == "name6").Value);
					Assert.Equal(17, identities.First(x => x.Key == "name7").Value);
				});
			}
		}

		[Theory]
		[PropertyData("Storages")]
        public void General_Initialized_WithoutErrors(string requestedStorage)
        {
            Assert.DoesNotThrow(() =>
            {
                using (var storage = NewTransactionalStorage(requestedStorage))
                {
                    storage.Batch(viewer => Assert.NotNull(viewer.General));
                }
            });
        }

		[Theory]
		[PropertyData("Storages")]
		public void General_SetIdentityValue_GetNextIdentityValue_CorrectResult(string requestedStorage)
        {
            const long INITIAL_VALUE = 123;
            const string IDENTITY_NAME = "Foo";

            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(mutator => mutator.General.SetIdentityValue(IDENTITY_NAME,INITIAL_VALUE));

                long nextValue = 0;
                storage.Batch(viewer => nextValue = viewer.General.GetNextIdentityValue(IDENTITY_NAME));

                Assert.Equal(INITIAL_VALUE + 1,nextValue);
            }
        }

		[Theory]
		[PropertyData("Storages")]
		public void General_GetNextIdentityValue_WithoutSet_CorrectResult(string requestedStorage)
        {
            const string IDENTITY_NAME = "Foo";
            const int GET_NEXT_INVOCATION_COUNT = 5;

            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                long nextValue = 0;
                for(int index = 0;index < GET_NEXT_INVOCATION_COUNT; index++)
                    storage.Batch(viewer => nextValue = viewer.General.GetNextIdentityValue(IDENTITY_NAME));

                Assert.Equal(GET_NEXT_INVOCATION_COUNT, nextValue);
            }
        }
    
    }
}
