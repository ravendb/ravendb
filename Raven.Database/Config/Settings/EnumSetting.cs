using System;

namespace Raven.Database.Config.Settings
{
    internal class EnumSetting<T> : Setting<T>
    {
        private readonly Type enumType;

        public EnumSetting(string value, T defaultValue) : base(value, defaultValue)
        {
            enumType = GetEnumType();
        }

        private static Type GetEnumType()
        {
            var type = typeof(T);

            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                type = type.GenericTypeArguments[0];

            if (type.IsEnum == false)
                throw new InvalidOperationException("EnumSetting can only be used for enums");

            return type;
        }

        public EnumSetting(string value, Func<T> getDefaultValue) : base(value, getDefaultValue)
        {
            enumType = GetEnumType();
        }

        public override T Value
        {
            get
            {
                if (string.IsNullOrEmpty(value) == false)
                {
                    return (T)Enum.Parse(enumType, value);
                }
                if (getDefaultValue != null)
                {
                    return getDefaultValue();
                }
                return defaultValue;
            }
        }
    }
}
