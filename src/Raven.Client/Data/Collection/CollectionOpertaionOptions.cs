using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Raven.Client.Data.Collection
{
    public class CollectionOpertaionOptions
    {
        private int? _maxOpsPerSecond;

        /// <summary>
        /// Limits the amount of base operation per second allowed.
        /// </summary>
        public int? MaxOpsPerSecond
        {
            get
            {
                return _maxOpsPerSecond;
            }

            set
            {
                if (value.HasValue && value.Value <= 0)
                    throw new InvalidOperationException("MaxOpsPerSecond must be greater than 0");

                _maxOpsPerSecond = value;
            }
        }
    }
}
