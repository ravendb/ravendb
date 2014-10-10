// -----------------------------------------------------------------------
//  <copyright file="NullableIntegerSetting.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Database.Config.Settings
{
	internal class NullableIntegerSetting : Setting<int?>
	{
		public NullableIntegerSetting(string value, int? defaultValue)
			: base(value, defaultValue)
		{
		}

		public NullableIntegerSetting(string value, Func<int?> getDefaultValue)
			: base(value, getDefaultValue)
		{
		}

		public override int? Value
		{
			get
			{
				return string.IsNullOrEmpty(value) == false
						   ? int.Parse(value)
						   : (getDefaultValue != null ? getDefaultValue() : defaultValue);
			}
		}
	}
}