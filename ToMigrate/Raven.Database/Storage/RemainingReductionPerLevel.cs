using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Database.Storage
{
    public class RemainingReductionPerLevel
    {
        public long RemainingReductionsLevel0 { get { return remainingReductionsLevel0; } }
        private long remainingReductionsLevel0;
        public long RemainingReductionsLevel1 { get { return remainingReductionsLevel1; } }
        private long remainingReductionsLevel1;
        public long RemainingReductionsLevel2 { get { return remainingReductionsLevel2; } }
        private long remainingReductionsLevel2;
        public RemainingReductionPerLevel() { }

        public RemainingReductionPerLevel(int level)
        {
            IncrementPerLevelCounters(level);
        }

        public RemainingReductionPerLevel IncrementPerLevelCounters(int level)
        {
            switch (level)
            {
                case 0:
                    Interlocked.Increment(ref remainingReductionsLevel0);
                    break;
                case 1:
                    Interlocked.Increment(ref remainingReductionsLevel1);
                    break;
                case 2:
                    Interlocked.Increment(ref remainingReductionsLevel2);
                    break;
            }
            return this;
        }

        public RemainingReductionPerLevel DecrementPerLevelCounters(int level)
        {
            switch (level)
            {
                case 0:
                    Interlocked.Decrement(ref remainingReductionsLevel0);
                    break;
                case 1:
                    Interlocked.Decrement(ref remainingReductionsLevel1);
                    break;
                case 2:
                    Interlocked.Decrement(ref remainingReductionsLevel2);
                    break;
            
            }
            return this;
        }
        public RemainingReductionPerLevel Add(RemainingReductionPerLevel other)
        {
            Interlocked.Add(ref remainingReductionsLevel0, other.remainingReductionsLevel0);
            Interlocked.Add(ref remainingReductionsLevel1, other.remainingReductionsLevel1);
            Interlocked.Add(ref remainingReductionsLevel2, other.remainingReductionsLevel2);
            return this;
        }
    }
}
