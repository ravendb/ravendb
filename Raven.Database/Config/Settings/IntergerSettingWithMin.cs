// -----------------------------------------------------------------------
//  <copyright file="IntergerSettingWithMin.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Database.Config.Settings
{
	public class IntergerSettingWithMin : Setting<int>
	{
		private readonly int min;

		public IntergerSettingWithMin(string value, int defaultValue, int min)
			: base(value, defaultValue)
		{
			this.min = min;
		}

		public override int Value
		{
			get
			{
				return value != null ? Math.Max(int.Parse(value), min) : defaultValue;
			}
		}

		public int Default
		{
			get { return defaultValue; }
		}
	}
}