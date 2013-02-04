// -----------------------------------------------------------------------
//  <copyright file="TimeSpanSetting.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Database.Config.Settings
{
	public enum TimeSpanArgumentType
	{
		Unknown = 0,
		FromSeconds = 1,
		FromParse = 2,
	}

	public class TimeSpanSetting : Setting<TimeSpan>
	{
		private readonly TimeSpanArgumentType argumentType;

		public TimeSpanSetting(string value, TimeSpan defaultValue, TimeSpanArgumentType argumentType) : base(value, defaultValue)
		{
			this.argumentType = argumentType;
		}

		//public TimeSpanSetting(string value, Func<TimeSpan> getDefaultValue) : base(value, getDefaultValue)
		//{
		//}

		public override TimeSpan Value
		{
			get
			{
				if (value != null)
				{
					switch (argumentType)
					{
						case TimeSpanArgumentType.FromParse:
							return TimeSpan.Parse(value);

						case TimeSpanArgumentType.FromSeconds:
							return TimeSpan.FromSeconds(int.Parse(value));
						default:
							throw new ArgumentException("Invalid TimeSpanArgumentType");
					}
				}

				return defaultValue;
			}
		}
	}
}