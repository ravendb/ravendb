using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;

namespace Raven.Studio.Controls
{
	public partial class DateTimePicker : UserControl
	{
		public DateTimePicker()
		{
			InitializeComponent();

			DatePicker.SelectedDateChanged += new EventHandler<SelectionChangedEventArgs>(DatePicker_SelectedDateChanged);
			TimePicker.ValueChanged += new RoutedPropertyChangedEventHandler<DateTime?>(TimePicker_ValueChanged);
		}

		public DateTime? SelectedDateTime
		{
			get
			{
				return (DateTime?)GetValue(SelectedDateTimeProperty);
			}
			set
			{
				SetValue(SelectedDateTimeProperty, value);
			}
		}

		public static readonly DependencyProperty SelectedDateTimeProperty =
			DependencyProperty.Register("SelectedDateTime",
			typeof(DateTime?),
			typeof(DateTimePicker),
			new PropertyMetadata(null, new PropertyChangedCallback(SelectedDateTimeChanged)));

		private static void SelectedDateTimeChanged(DependencyObject sender, DependencyPropertyChangedEventArgs e)
		{
			DateTimePicker me = sender as DateTimePicker;

			if (me != null)
			{
				me.DatePicker.SelectedDate = (DateTime?)e.NewValue;
				me.TimePicker.Value = (DateTime?)e.NewValue;
			}
		}

		private void TimePicker_ValueChanged(object sender, RoutedPropertyChangedEventArgs<DateTime?> e)
		{
			if (DatePicker.SelectedDate != TimePicker.Value)
			{
				DatePicker.SelectedDate = TimePicker.Value;
			}

			if (SelectedDateTime != TimePicker.Value)
			{
				SelectedDateTime = TimePicker.Value;
			}
		}

		private void DatePicker_SelectedDateChanged(object sender, SelectionChangedEventArgs e)
		{
			// correct the new date picker date by the time picker's time
			if (DatePicker.SelectedDate.HasValue && TimePicker.Value.HasValue)
			{
				// get both values
				DateTime datePickerDate = DatePicker.SelectedDate.Value;
				DateTime timePickerDate = TimePicker.Value.Value;

				// compare relevant parts manually
				if (datePickerDate.Hour != timePickerDate.Hour
					|| datePickerDate.Minute != timePickerDate.Minute
					|| datePickerDate.Second != timePickerDate.Second)
				{
					// correct the date picker value
					DatePicker.SelectedDate = new DateTime(datePickerDate.Year,
						datePickerDate.Month,
						datePickerDate.Day,
						timePickerDate.Hour,
						timePickerDate.Minute,
						timePickerDate.Second);

					// return, because this event handler will be executed a second time
					return;
				}
			}

			// now transfer the date picker's value to the time picker
			// and dependency property
			if (TimePicker.Value != DatePicker.SelectedDate)
			{
				TimePicker.Value = DatePicker.SelectedDate;
			}

			if (SelectedDateTime != DatePicker.SelectedDate)
			{
				SelectedDateTime = DatePicker.SelectedDate;
			}
		}
	}
}
