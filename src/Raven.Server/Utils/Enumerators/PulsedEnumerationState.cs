using Raven.Server.ServerWide.Context;
using Sparrow;
using Voron.Impl;

namespace Raven.Server.Utils.Enumerators
{
    public abstract class PulsedEnumerationState<T>
    {
        internal const int NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded = 1024;

        protected readonly DocumentsOperationContext Context;

        private readonly Size _pulseLimit;

        protected PulsedEnumerationState(DocumentsOperationContext context, Size pulseLimit)
        {
            Context = context;
            _pulseLimit = pulseLimit;
        }

        public int ReadCount;

        public virtual bool ShouldPulseTransaction()
        {
            if (ReadCount > 0 && ReadCount % NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded == 0)
            {
                var size = Context.Transaction.InnerTransaction.LowLevelTransaction.GetTotal32BitsMappedSize() +
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
