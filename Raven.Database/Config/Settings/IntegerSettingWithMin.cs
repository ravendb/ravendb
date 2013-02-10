// -----------------------------------------------------------------------
//  <copyright file="IntegerSettingWithMin.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Database.Config.Settings
{
	public class IntegerSettingWithMin : Setting<int>
	{
		private readonly int min;

		public IntegerSettingWithMin(string value, int defaultValue, int min)
			: base(value, defaultValue)
		{
			this.min = min;
		}

		public override int Value
		{
			get
			{
				return string.IsNullOrEmpty(value) == false ? Math.Max(int.Parse(value), min) : defaultValue;
			}
		}

		public int Default
		{
			get { return defaultValue; }
		}
	}
}