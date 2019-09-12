using Raven.Server.ServerWide.Context;
using Sparrow;
using Voron.Impl;

namespace Raven.Server.Utils.Enumerators
{
    public abstract class PulsedEnumerationState<T>
    {
        protected readonly DocumentsOperationContext Context;

        protected PulsedEnumerationState(DocumentsOperationContext context)
        {
            Context = context;
        }

        protected readonly Size PulseLimit = new Size(16, SizeUnit.Megabytes); // TODO arek - make it configurable


        public int ReadCount { get; set; }

        public virtual bool ShouldPulseTransaction()
        {
            if (ReadCount % 1024 == 0)
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

        public abstract void OnMoveNext(T current);
    }
}
