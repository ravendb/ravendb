using System;

namespace Raven.Database.Config.Settings
{
	internal class EnumSetting<T> : Setting<T>
	{
		public EnumSetting(string value, T defaultValue) : base(value, defaultValue)
		{
			if(typeof(T).IsEnum == false)
				throw new InvalidOperationException("EnumSetting can only be used for enums");
		}

		public EnumSetting(string value, Func<T> getDefaultValue) : base(value, getDefaultValue)
		{
			if (typeof(T).IsEnum == false)
				throw new InvalidOperationException("EnumSetting can only be used for enums");
		}

		public override T Value
		{
			get
			{
				if (string.IsNullOrEmpty(value) == false)
				{
					return (T) Enum.Parse(typeof (T), value);
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