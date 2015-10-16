// -----------------------------------------------------------------------
//  <copyright file="SizeSetting.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

namespace Raven.Database.Config.Settings
{
	public struct SizeSetting
	{
		public static readonly Type TypeOf = typeof(SizeSetting);

		private const long OneKb = 1024;
		private const long OneMb = OneKb * 1024;
		private const long OneGb = OneMb * 1024;
		private const long OneTb = OneGb * 1024;

		private readonly SizeUnit unit;
		private readonly long value;
		private readonly long valueInBytes;

		public SizeSetting(long value, SizeUnit unit)
		{
			this.value = value;
			this.unit = unit;

			switch (unit)
			{
				case SizeUnit.Bytes:
					valueInBytes = value;
					break;
				case SizeUnit.Kilobytes:
					valueInBytes = value * OneKb;
					break;
				case SizeUnit.Megabytes:
					valueInBytes = value * OneMb;
					break;
				case SizeUnit.Gigabytes:
					valueInBytes = value * OneGb;
					break;
				case SizeUnit.Terabytes:
					valueInBytes = value * OneTb;
					break;
				default:
					throw new NotSupportedException("Not supported size unit: " + unit);
			}
		}

		public long Bytes => valueInBytes;
		public long Kilobytes => valueInBytes / OneKb;
		public long Megabytes => valueInBytes / OneMb;
		public long Gigabytes => valueInBytes / OneGb;
		public long Terabytes => valueInBytes / OneTb;
	}

	public enum SizeUnit
	{
		Bytes,
		Kilobytes,
		Megabytes,
		Gigabytes,
		Terabytes
	}
}