using System.Threading;
using Sparrow;
using Sparrow.Server.LowMemory;

namespace Voron.Impl.Scratch
{
    /// <summary>
    /// Anonymous memory for the scratch buffers of many environments: with no file behind it, the kernel has nothing to
    /// write its dirty pages back to. Without swap it cannot be reclaimed either, so it is limited to keep the OOM killer
    /// away. When the budget is used up, scratch pages go to files, and the environments that hold more than one segment
    /// give back their last one once no transaction uses it (see ScratchBufferPool).
    /// </summary>
    public sealed class ScratchMemoryBudget
    {
        public const double DefaultFractionOfPhysicalMemory = 0.1;

        // pressure ends when the used memory falls to this part of the limit, so a single segment does not flip it
        private const double PressureEndsBelow = 0.9;

        /// <summary>
        /// Shared by all environments in the process, unless the options name another budget.
        /// </summary>
        public static readonly ScratchMemoryBudget Global = new();

        private long _limit;
        private long _used;
        private int _underPressure;

        public ScratchMemoryBudget(long limit = -1)
        {
            _limit = limit < 0 ? -1 : limit;
        }

        /// <summary>
        /// The limit in bytes, by default <see cref="DefaultFractionOfPhysicalMemory"/> of the physical memory
        /// (or of the cgroup limit). Set a negative value to go back to the default.
        /// </summary>
        public long Limit
        {
            get
            {
                var limit = Volatile.Read(ref _limit);
                if (limit >= 0)
                    return limit;

                limit = (long)(MemoryInformation.TotalPhysicalMemory.GetValue(SizeUnit.Bytes) * DefaultFractionOfPhysicalMemory);
                Interlocked.CompareExchange(ref _limit, limit, -1);
                return Volatile.Read(ref _limit);
            }
            set => Volatile.Write(ref _limit, value < 0 ? -1 : value);
        }

        public long Used => Volatile.Read(ref _used);

        /// <summary>
        /// A reservation failed, and the used memory has not fallen back yet.
        /// </summary>
        public bool UnderPressure => Volatile.Read(ref _underPressure) != 0;

        public bool TryReserve(long bytes)
        {
            var limit = Limit;
            while (true)
            {
                var used = Volatile.Read(ref _used);
                if (used + bytes > limit)
                {
                    if (limit > 0)
                        Volatile.Write(ref _underPressure, 1);
                    return false;
                }

                if (Interlocked.CompareExchange(ref _used, used + bytes, used) == used)
                    return true;
            }
        }

        public void Release(long bytes)
        {
            var used = Interlocked.Add(ref _used, -bytes);
            if (used <= Limit * PressureEndsBelow)
                Volatile.Write(ref _underPressure, 0);
        }
    }
}
