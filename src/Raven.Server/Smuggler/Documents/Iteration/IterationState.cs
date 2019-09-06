using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Enumerators;
using Sparrow;
using Voron.Impl;

namespace Raven.Server.Smuggler.Documents.Iteration
{
    public abstract class IterationState<T> : PulsedEnumerationState<T>
    {
        protected IterationState(DocumentsOperationContext context)
        {
            Context = context;
        }

        protected readonly Size PulseLimit = new Size(16, SizeUnit.Megabytes); // TODO arek - make it configurable

        protected readonly DocumentsOperationContext Context;

        public int ReadCount { get; set; }

        public override bool ShouldPulseTransaction()
        {
            if (ReadCount == 3)
            {
                var size = Context.Transaction.InnerTransaction.LowLevelTransaction.GetTotal32BitsMappedSize() +
                           Context.Transaction.InnerTransaction.LowLevelTransaction.TotalEncryptionBufferSize;

                if (size > PulseLimit)
                {
                    return true;
                }
            }

            return false;
        }
    }
}
