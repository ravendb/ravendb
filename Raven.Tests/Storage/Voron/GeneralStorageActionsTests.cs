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
