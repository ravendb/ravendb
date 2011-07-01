// -----------------------------------------------------------------------
//  <copyright file="CanGetStats.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.ComponentModel.Composition;
using System.Text.RegularExpressions;
using System.Windows;
using System.Windows.Interop;

namespace Raven.Studio.Infrastructure.Navigation
{
	[Export]
	public class NavigationService
	{
		[ImportMany]
		public Lazy<INavigator, INavigatorMetdata>[] Routes { get; set; }

		public void Initialize()
		{
			Application.Current.Host.NavigationStateChanged += (sender, args) => Navigate(args);
		}

		private void Navigate(NavigationStateChangedEventArgs e)
		{
			foreach (var route in Routes)
			{
				var match = Regex.Match(e.NewNavigationState, route.Metadata.Url);
				if (match.Success == false)
					continue;

				route.Value.Navigate(match.Groups.Count > 1 ? match.Groups[1].Value : null);
			}
		}

		public void Track(string navigationState)
		{
			Application.Current.Host.NavigationState = navigationState;
		}
	}
}