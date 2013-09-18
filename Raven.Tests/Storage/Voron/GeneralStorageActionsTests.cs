using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Database.Storage;
using Xunit;

namespace Raven.Tests.Storage.Voron
{
	[Trait("VoronTest", "StorageActionsTests")]
	public class GeneralStorageActionsTests : RavenTest
    {
        private ITransactionalStorage NewVoronStorage()
        {
            return NewTransactionalStorage("voron");
        }

        [Fact]
        public void General_Initialized_WithoutErrors()
        {
            Assert.DoesNotThrow(() =>
            {
                using (var storage = NewVoronStorage())
                {
                    storage.Batch(viewer => Assert.NotNull(viewer.General));
                }
            });
        }

        [Fact]
        public void General_SetIdentityValue_GetNextIdentityValue_CorrectResult()
        {
            const long INITIAL_VALUE = 123;
            const string IDENTITY_NAME = "Foo";

            using (var storage = NewVoronStorage())
            {
                storage.Batch(mutator => mutator.General.SetIdentityValue(IDENTITY_NAME,INITIAL_VALUE));

                long nextValue = 0;
                storage.Batch(viewer => nextValue = viewer.General.GetNextIdentityValue(IDENTITY_NAME));

                Assert.Equal(INITIAL_VALUE + 1,nextValue);
            }
        }

        [Fact]
        public void General_GetNextIdentityValue_WithoutSet_CorrectResult()
        {
            const string IDENTITY_NAME = "Foo";
            const int GET_NEXT_INVOCATION_COUNT = 5;

            using (var storage = NewVoronStorage())
            {
                long nextValue = 0;
                for(int index = 0;index < GET_NEXT_INVOCATION_COUNT; index++)
                    storage.Batch(viewer => nextValue = viewer.General.GetNextIdentityValue(IDENTITY_NAME));

                Assert.Equal(GET_NEXT_INVOCATION_COUNT, nextValue);
            }
        }
    
    }
}
