using System;
using System.Collections.Generic;
using System.Windows.Media;

namespace Raven.Studio.Features.Documents
{
	public class TemplateColorProvider
	{
		public static TemplateColorProvider Instance = new TemplateColorProvider();
		readonly Dictionary<string, Brush> colors = new Dictionary<string, Brush>(StringComparer.OrdinalIgnoreCase);

		public TemplateColorProvider()
		{
			colors.Add("Sys doc", new SolidColorBrush(new Color { R = 0x12, G = 0x3c, B = 0x65, A = 0xff }));
			colors.Add("Orphans", new SolidColorBrush(new Color { R = 0xbf, G = 0x40, B = 0x40, A = 0xff }));
		}

		public Brush ColorFrom(string key)
		{
			if (!colors.ContainsKey(key))
			{
				colors[key] = new LinearGradientBrush(GetStopCollection(), 90);
			}

			return colors[key];
		}

		private GradientStopCollection GetStopCollection()
		{
			var collection = new GradientStopCollection();
			var numberOfSections = (colors.Count - 2) / AllColors.Count + 1;
			var stops = new List<GradientStop>();
			double stepSize;
			if(numberOfSections == 1)
				stepSize = 0;
			else
				stepSize = 1/((double)numberOfSections - 1);

			for (var i = 0; i < numberOfSections; i++)
			{
				var stop = new GradientStop {Color = NextFreeColor(i), Offset = (stepSize * i)};
				stops.Add(stop);
			}
			foreach (var gradientStop in stops)
			{
				collection.Add(gradientStop);
			}

			return collection;
		}

		public Color NextFreeColor(int addition = 0)
		{
			var index = (colors.Count - 2 + addition) % AllColors.Count;

			return AllColors[index];
		}

		private static readonly List<Color> AllColors = new List<Color>
		{
			Color.FromArgb(255, 0,200,0),
			Color.FromArgb(255, 200,0,255),
			Color.FromArgb(255, 255,0,0),
			Color.FromArgb(255, 0,150,240),
			Color.FromArgb(255, 168,0,104),
			Color.FromArgb(255, 0,100,100),
			Color.FromArgb(255, 120,50,200),
			Color.FromArgb(255, 200,40,40),
			Color.FromArgb(255, 200,200,0),
			Color.FromArgb(255, 255,129,0),
			Color.FromArgb(255, 0,150,255),
			Color.FromArgb(255, 150,255,200),
			Color.FromArgb(255, 150,0,255),
			Color.FromArgb(255, 200,255,0),
			Color.FromArgb(255, 168,129,0),
		};
	}
}