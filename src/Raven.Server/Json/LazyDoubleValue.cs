using System.Globalization;

namespace Raven.Server.Json
{
    public class LazyDoubleValue
    {
        public readonly LazyStringValue Inner;
        private double? _val;
        public LazyDoubleValue(LazyStringValue inner)
        {
            Inner = inner;
        }

        public static implicit operator double(LazyDoubleValue self)
        {
            if (self._val != null)
                return self._val.Value;

            var val = double.Parse(self.Inner, CultureInfo.InvariantCulture);
            self._val = val;
            return val;
        }

    }
}