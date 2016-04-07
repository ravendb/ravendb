using System.Globalization;

namespace Sparrow.Json
{
    public class LazyDoubleValue
    {
        public readonly LazyStringValue Inner;
        private double? _val;

        private double Val => _val.GetValueOrDefault();

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