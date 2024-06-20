using Raven.Server.ServerWide.Context;
using Sparrow;
using Voron.Impl;

namespace Raven.Server.Utils.Enumerators
{
    public abstract class PulsedEnumerationState<T>
    {
        public const int DefaultNumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded = 1024;

        private readonly int _numberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded;

        protected readonly DocumentsOperationContext Context;

        private readonly Size _pulseLimit;

        protected PulsedEnumerationState(
            DocumentsOperationContext context,
            Size pulseLimit,
            int numberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded = DefaultNumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded)
        {
            Context = context;
            _pulseLimit = pulseLimit;
            _numberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded = numberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded;
        }

        public int ReadCount;

        public virtual bool ShouldPulseTransaction()
        {
            if (ReadCount > 0 && ReadCount % _numberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded == 0)
            {
                var size = Context.Transaction.InnerTransaction.LowLevelTransaction.PagerTransactionState.GetTotal32BitsMappedSize() +
                           Context.Transaction.InnerTransaction.LowLevelTransaction.AdditionalMemoryUsageSize;

                if (size >= _pulseLimit)
                {
                    return true;
                }
            }

            return false;
        }

        public abstract void OnMoveNext(T current);
    }
}
