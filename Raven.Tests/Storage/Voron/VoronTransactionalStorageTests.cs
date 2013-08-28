using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Storage.Voron
{
    public class VoronTransactionalStorageTests : RavenTest
    {
        [Fact]
        public void TransactionalStorageInitialized_CorrectStorageInitialized_DocumentStorageActions_Is_NotNull()
        {
            using (var voronStorage = NewTransactionalStorage("voron"))
            {                
                Assert.Equal(voronStorage.FriendlyName,"Voron");
                voronStorage.Batch(accessor => Assert.NotNull(accessor.Documents));
            }
        }
    }
}
