// -----------------------------------------------------------------------
//  <copyright file="Setting.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Database.Config.Settings
{
	public abstract class Setting<T>
	{
		protected readonly string value;
		protected readonly Func<T> getDefaultValue;
		protected readonly T defaultValue;

		protected Setting(string value, T defaultValue)
		{
			this.value = value;
			this.defaultValue = defaultValue;
		}

		protected Setting(string value, Func<T> getDefaultValue)
		{
			this.value = value;
			this.getDefaultValue = getDefaultValue;
		}

		public abstract T Value { get; }
	}
}