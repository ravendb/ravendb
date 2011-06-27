// -----------------------------------------------------------------------
//  <copyright file="CanGetStats.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Windows;
using System.Windows.Controls;

namespace Raven.Studio.Helpers
{
	public class StateManager : DependencyObject
	{
		public static string GetVisualStateProperty(DependencyObject obj)
		{
			return (string) obj.GetValue(VisualStatePropertyProperty);
		}

		public static void SetVisualStateProperty(DependencyObject obj, string value)
		{
			obj.SetValue(VisualStatePropertyProperty, value);
		}

		public static readonly DependencyProperty VisualStatePropertyProperty =
			DependencyProperty.RegisterAttached(
				"VisualStateProperty",
				typeof (string),
				typeof (StateManager),
				new PropertyMetadata((s, e) =>
				                     	{
				                     		var propertyName = (string) e.NewValue;
				                     		var ctrl = s as Control;
				                     		if (ctrl == null)
				                     			throw new InvalidOperationException(
				                     				"This attached property only supports types derived from Control.");
											VisualStateManager.GoToState(ctrl, propertyName, true);
				                     	}));
	}
}