// -----------------------------------------------------------------------
//  <copyright file="StringSetting.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Database.Config.Settings
{
	public class StringSetting : Setting<string>
	{
		public StringSetting(string value, string defaultValue) : base(value, defaultValue)
		{
		}

		public StringSetting(string value, Func<string> getDefaultValue) : base(value, getDefaultValue)
		{
		}

		public override string Value
		{
			get
			{
				if (string.IsNullOrEmpty(value) == false)
				{
					return value;
				}
				if(string.IsNullOrEmpty(defaultValue) == false)
				{
					return defaultValue;
				}
				if (getDefaultValue != null)
				{
					return getDefaultValue();
				}
				return null;
			}
		}
	}
}