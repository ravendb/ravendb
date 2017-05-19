using System;

namespace Raven.Client.Documents.Operations
{
    public class CollectionOperationOptions
    {
        private int? _maxOpsPerSecond;

        /// <summary>
        /// Limits the amount of base operation per second allowed.
        /// </summary>
        public int? MaxOpsPerSecond
        {
            get => _maxOpsPerSecond;

            set
            {
                if (value.HasValue && value.Value <= 0)
                    throw new InvalidOperationException("MaxOpsPerSecond must be greater than 0");

                _maxOpsPerSecond = value;
            }
        }
    }
}