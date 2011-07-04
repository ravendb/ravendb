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
using System.Windows.Browser;
using System.Windows.Interop;
using Caliburn.Micro;
using Raven.Studio.Features.Database;
using Raven.Studio.Infrastructure.Navigation.Navigators;

namespace Raven.Studio.Infrastructure.Navigation
{
	[Export]
	public class NavigationService
	{
		private string currentUrl;
		private Regex databasesRegEx = new Regex("^databases/([^/]+)");

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

			string database = Server.DefaultDatabaseName;
			var databasesMatch = databasesRegEx.Match(currentUrl);
			if (databasesMatch.Success)
			{
				currentUrl = currentUrl.Substring(databasesMatch.Length);
				database = databasesMatch.Groups[1].Value;
			}

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
				route.Value.Navigate(database, parameters);
				return;
			}
		}

		public void Track(NavigationState navigationState)
		{
			if (navigationState.Url == currentUrl)
				return;
			currentUrl = navigationState.Url;

			// Application.Current.IsRunningOutOfBrowser
			Application.Current.Host.NavigationState = navigationState.Url;
			HtmlPage.Document.SetProperty("title", navigationState.Title);
		}
	}

	public class NavigationState
	{
		public string Url { get; set; }
		public string Title { get; set; }
	}
}