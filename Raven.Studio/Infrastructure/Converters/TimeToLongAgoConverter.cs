using System;
using System.Globalization;
using System.Windows.Data;
using Raven.Abstractions;

namespace Raven.Studio.Infrastructure.Converters
{
	public class TimeToLongAgoConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			if (value is DateTime)
			{
				var timeAgo = SystemTime.UtcNow - (DateTime)value;

				if (timeAgo.TotalDays >= 1)
					return string.Format("{0:#,#} days ago", timeAgo.TotalDays);
				if (timeAgo.TotalHours >= 1)
					return string.Format("{0:#,#} hours ago", timeAgo.TotalHours);
				if (timeAgo.TotalMinutes >= 1)
					return string.Format("{0:#,#} minutes ago", timeAgo.TotalMinutes);
				if (timeAgo.TotalSeconds >= 1)
					return string.Format("{0:#,#} seconds ago", timeAgo.TotalSeconds);

				return string.Format("{0:#,#} milli-seconds ago", timeAgo.TotalMilliseconds);
			}
			return null;

		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}