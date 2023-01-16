using Raven.Server.ServerWide.Context;
using Sparrow;
using Voron.Impl;

namespace Raven.Server.Utils.Enumerators
{
    public abstract class PulsedEnumerationState<T>
    {
        public const int DefaultNumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded = 1024;

        protected int NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded = 1024;

        protected readonly DocumentsOperationContext Context;
        protected Size PulseLimit { get; private set; }

        protected PulsedEnumerationState(DocumentsOperationContext context, Size pulseLimit)
        {
            Context = context;
            PulseLimit = pulseLimit;
        }

        public int ReadCount;

        public virtual bool ShouldPulseTransaction()
        {
            if (ReadCount > 0 && ReadCount % NumberOfEnumeratedDocumentsToCheckIfPulseLimitExceeded == 0)
            {
                var size = Context.Transaction.InnerTransaction.LowLevelTransaction.GetTotal32BitsMappedSize() +
                           Context.Transaction.InnerTransaction.LowLevelTransaction.AdditionalMemoryUsageSize;

                if (size >= PulseLimit)
                {
                    return true;
                }
            }

            return false;
        }

        public abstract void OnMoveNext(T current);
    }
}
