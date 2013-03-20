using System;
using System.Globalization;
using System.Windows.Data;
using Raven.Studio.Models;

namespace Raven.Studio.Infrastructure.Converters
{
	public class TaskStatusConverter : IValueConverter
	{
		public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
		{
			if (value is TaskStatus == false)
				return "";

			var status = (TaskStatus)value;

			switch (status)
			{
					case TaskStatus.DidNotStart:
					return "";

					case TaskStatus.Started:
					return "Task in progress";

					case TaskStatus.Ended:
					return "Task finished";

				default:
					return "";
			}
		}

		public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
		{
			throw new NotImplementedException();
		}
	}
}
