using System.Threading;

namespace Metrics.Utils
{
    internal struct VolatileDouble
    {
        private double value;

        public VolatileDouble(double value)
            : this()
        {
            this.value = value;
        }

        public void Set(double value)
        {
            Volatile.Write(ref this.value, value);
        }

        public double Get()
        {
            return Volatile.Read(ref this.value);
        }
    }
}
