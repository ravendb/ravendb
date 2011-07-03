// -----------------------------------------------------------------------
//  <copyright file="CanGetStats.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Text.RegularExpressions;
using System.Windows;
using System.Windows.Interop;

namespace Raven.Studio.Infrastructure.Navigation
{
	[Export]
	public class NavigationService
	{
		private string currentUrl;

		[ImportMany]
		public Lazy<INavigator, INavigatorMetdata>[] Routes { get; set; }

		public void Initialize()
		{
			Application.Current.Host.NavigationStateChanged += (sender, args) => Navigate(args);
		}

		private void Navigate(NavigationStateChangedEventArgs e)
		{
			if (e.NewNavigationState == currentUrl)
				return;
			currentUrl = e.NewNavigationState;
			foreach (var route in Routes)
			{
				var regex = new Regex(route.Metadata.Url);
				var match = regex.Match(e.NewNavigationState);
				if (match.Success == false)
					continue;

				var parameters = new Dictionary<string, string>();
				foreach (var name in regex.GetGroupNames())
				{
					parameters[name] = match.Groups[name].Value;
				}
				route.Value.Navigate(parameters);
			}
		}

		public void Track(string navigationState)
		{
			if (navigationState == currentUrl)
				return;
			currentUrl = navigationState;
			Application.Current.Host.NavigationState = navigationState;
		}
	}
}