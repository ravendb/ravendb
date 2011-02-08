namespace Raven.Studio.Framework
{
	using System;
	using System.Collections.Generic;
	using System.Diagnostics;
	using System.Windows.Media;

	public class TemplateColorProvider
	{
		const double GoldenAngle = 0.381966;

		//readonly List<double> simple = new List<double>{0.33,0.67, 0.83};

		readonly Dictionary<string, double> baseHues = new Dictionary<string, double>();
		readonly Dictionary<string, Color> colors = new Dictionary<string, Color>();

		public Color ColorFrom(string key)
		{
			if (!colors.ContainsKey(key))
			{
				var s =  0.41;
				var v = 0.88;
				var h = BaseHueFor(key);

				colors[key] = ColorFromHSV(h, s, v);

				Debug.WriteLine("create color for " + key + " h: " + h);
			}
			return colors[key];
		}

		double BaseHueFor(string key)
		{
			if (!baseHues.ContainsKey(key))
			{
				var index = baseHues.Count;
				var angle = index*GoldenAngle;
				var hue = angle - Math.Floor(angle);
				baseHues[key] = hue;
				//baseHues[outlet] = simple[index];
			}

			return baseHues[key];
		}

		public static Color ColorFromHSV(double hue, double saturation, double value)
		{
			var hi = Convert.ToInt32(Math.Floor(hue*6))%6;

			value = value*255;
			var v = Convert.ToByte(value);
			var p = Convert.ToByte(value*(1 - saturation));
			var q = Convert.ToByte(value*(1 - hue*saturation));
			var t = Convert.ToByte(value*(1 - (1 - hue)*saturation));

			switch (hi)
			{
				case 0:
					return Color.FromArgb(255, v, t, p);
				case 1:
					return Color.FromArgb(255, q, v, p);
				case 2:
					return Color.FromArgb(255, p, v, t);
				case 3:
					return Color.FromArgb(255, p, q, v);
				case 4:
					return Color.FromArgb(255, t, p, v);
				default:
					return Color.FromArgb(255, v, p, q);
			}
		}
	}
}