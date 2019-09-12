using Raven.Server.ServerWide.Context;
using Sparrow;
using Voron.Impl;

namespace Raven.Server.Utils.Enumerators
{
    public abstract class PulsedEnumerationState<T>
    {
        protected readonly DocumentsOperationContext Context;
        private readonly Size _pulseLimit;

        protected PulsedEnumerationState(DocumentsOperationContext context, Size pulseLimit)
        {
            Context = context;
            _pulseLimit = pulseLimit;
        }

        public int ReadCount { get; set; }

        public virtual bool ShouldPulseTransaction()
        {
            if (ReadCount % 1024 == 0)
            {
                var size = Context.Transaction.InnerTransaction.LowLevelTransaction.GetTotal32BitsMappedSize() +
                           Context.Transaction.InnerTransaction.LowLevelTransaction.TotalEncryptionBufferSize;

                if (size > _pulseLimit)
                {
                    return true;
                }
            }

            return false;
        }

        public abstract void OnMoveNext(T current);
    }
}
