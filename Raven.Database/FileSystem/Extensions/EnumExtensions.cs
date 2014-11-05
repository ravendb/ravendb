using System;
using System.ComponentModel;

namespace Raven.Database.Server.RavenFS.Extensions
{
	public static class EnumExtensions
	{
		public static string GetDescription(this object enumerationValue)
		{
			var type = enumerationValue.GetType();
			if (!type.IsEnum)
			{
				throw new ArgumentException("EnumerationValue must be of Enum type", "enumerationValue");
			}

			var memberInfo = type.GetMember(enumerationValue.ToString());
			if (memberInfo.Length > 0)
			{
				var attributes = memberInfo[0].GetCustomAttributes(typeof(DescriptionAttribute), false);

				if (attributes.Length > 0)
				{
					return ((DescriptionAttribute)attributes[0]).Description;
				}
			}

			return enumerationValue.ToString();
		}
	}
}