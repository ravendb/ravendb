// -----------------------------------------------------------------------
//  <copyright file="MultipliedIntegerSetting.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Config.Settings
{
	public class MultipliedIntegerSetting
	{
		private readonly Setting<int> setting;
		private readonly int factor;

		public MultipliedIntegerSetting(Setting<int> setting, int factor)
		{
			this.setting = setting;
			this.factor = factor;
		}

		public int Value
		{
			get
			{
				return setting.Value * factor;
			}
		}
	}
}