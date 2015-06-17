using System;
using System.Diagnostics.Contracts;
using System.Linq;
using Voron;
using Voron.Util.Conversion;

namespace Raven.Database.TimeSeries
{
	public class SeriesType
	{
		private SeriesType(string type, byte size)
		{
			Types = new string[1] {type};
			Sizes = new byte[1] {size};
			Size = size;

			// TODO: Compute the size automatically based on the type
		}

		private SeriesType(string[] types, byte[] sizes, byte size)
		{
			Types = types;
			Sizes = sizes;
			Size = size;
		}

		private byte[] valBuffer;

		public string[] Types { get; set; }

		public byte[] Sizes { get; set; }
		
		public byte Size { get; set; }

		public static SeriesType Int
		{
			get { return new SeriesType("int", sizeof (int)); }
		}

		public static SeriesType Long
		{
			get { return new SeriesType("long", sizeof (long)); }
		}


		public static SeriesType Char(byte length)
		{
			var size = length * sizeof(char);
			return new SeriesType("char[" + length + "]", (byte)size);
		}


		public static SeriesType Simple()
		{
			return new SeriesType("double", sizeof(double));
		}

		public static SeriesType Custom(string name, params SeriesType[] seriesTypes)
		{
			string[] types = new string[seriesTypes.Length];
			byte[] sizes = new byte[seriesTypes.Length];
			byte size = 0;
			bool emptyName = name == null;
			for (int i = 0; i < seriesTypes.Length; i++)
			{
				var seriesType = seriesTypes[i];
				if (type == null)
				{
					type = seriesType.Type;
					size = seriesType.Size;
				}
				else
				{
					type += "," + seriesType.Type;
					size += seriesType.Size;
				}
				types[i] = seriesType.Types[0];
				sizes[i] = seriesType.Size;
				size += seriesType.Size;
			}
			return new SeriesType(type, size, sizes);
		}

		public class SeriesTypeBuilder
		{
			public void Write(double d)
			{
				
			}

			public void Write(string s)
			{
				
			}

			public void Write(double lng, double lat)
			{
				
			}
		}
	}
}