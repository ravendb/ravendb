using System;
using System.Globalization;
using System.Linq;

namespace Raven.Server.Config.Settings
{
    public struct FlagSetting<T> where T : struct, IConvertible
    {
        public static readonly Type TypeOf = typeof(T);
        public static readonly Type NullableTypeOf = typeof(T?);

        public FlagSetting(string settingRaw)
        {
            SettingValue = null;

            if (string.IsNullOrWhiteSpace(settingRaw))
            {
                return;
            }

            var values = settingRaw.Split('|')
                .Select(x => x.Trim())
                .Select(x => (T)Enum.Parse(typeof(T), x, true))
                .ToList();

            int result = 0;
            foreach (var value in values)
            {
                result = result | value.ToInt32(CultureInfo.InvariantCulture);
            }

            SettingValue = (T)(object)result;
        }

        public static implicit operator FlagSetting<T>(string s)
        {
            return new FlagSetting<T>(s);
        }

        public readonly T? SettingValue;
    }
}
