// -----------------------------------------------------------------------
//  <copyright file="IntegerSetting.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Database.Config.Settings
{
	public class IntegerSetting : Setting<int>
	{
		public IntegerSetting(string value, int defaultValue)
			: base(value, defaultValue)
		{
		}

		public IntegerSetting(string value, Func<int> getDefaultValue) : base(value, getDefaultValue)
		{
		}

		public override int Value
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