using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Voron;
using Xunit;

namespace Tryouts
{
    public class BasicCluster
    {
        [Fact]
        public void CanSetupSingleNode()
        {
            using (
                var consensus = new RachisConsensus(StorageEnvironmentOptions.CreateMemoryOnly(),
                    "https://localhost:8888"))
            {
                consensus.Initialize(new NoopStateMachine());

            }
        }
    }

    public class NoopStateMachine : RachisStateMachine
    {
        protected override void Apply(TransactionOperationContext context, BlittableJsonReaderObject cmd)
        {
            
        }
    }
}