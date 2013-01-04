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
				var dateTime = (DateTime) value;
				switch (dateTime.Kind)
				{
					case DateTimeKind.Local:
						dateTime = dateTime.ToUniversalTime();
						break;
					case DateTimeKind.Unspecified:
						dateTime = DateTime.SpecifyKind(dateTime, DateTimeKind.Utc);
						break;
				}
				var timeAgo = SystemTime.UtcNow - dateTime;

				if (timeAgo.TotalDays >= 1)
					return string.Format("{0:#,#} days ago", timeAgo.TotalDays);
				if (timeAgo.TotalHours >= 1)
					return string.Format("{0:#,#} hours ago", timeAgo.TotalHours);
				if (timeAgo.TotalMinutes >= 1)
					return string.Format("{0:#,#} minutes ago", timeAgo.TotalMinutes);
				if (timeAgo.TotalSeconds >= 1)
					return string.Format("{0:#,#} seconds ago", timeAgo.TotalSeconds);

				return string.Format("{0:#,#} milliseconds ago", timeAgo.TotalMilliseconds);
			}
			return null;

		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}