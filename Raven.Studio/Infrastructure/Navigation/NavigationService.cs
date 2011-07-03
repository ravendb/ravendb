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
using Caliburn.Micro;
using Raven.Studio.Framework;

namespace Raven.Studio.Infrastructure.Navigation
{
	[Export]
	public class NavigationService
	{
		private static Dictionary<Type, string> routes;
		public static Dictionary<Type, string> Routes
		{
			get { return routes ?? (routes = new Dictionary<Type, string>()); }
		}

		public void Initialize()
		{
			Application.Current.Host.NavigationStateChanged += (sender, args) => Navigate(args);
		}

		private void Navigate(NavigationStateChangedEventArgs e)
		{
			foreach (var route in Routes)
			{
				if (isUrlMatchsRoute(e.NewNavigationState, route.Value) == false)
					continue;

				var viewModel = (RavenScreen)IoC.GetInstance(route.Key, null);
			}
		}

		private bool isUrlMatchsRoute(string url, string routeSignature)
		{
			return true;
		}

		/// <summary>
		/// Track the URL to the current view model type's URL.
		/// </summary>
		/// <param name="viewModelType">The type of the view model to track.</param>
		/// <param name="parameters">The navigation parameters.</param>
		public void Track(Type viewModelType, Dictionary<string, string> parameters)
		{
			Application.Current.Host.NavigationState = GetUrlForViewModel(viewModelType, parameters);
		}

		private string GetUrlForViewModel(Type viewModelType, Dictionary<string, string> parameters)
		{
			if (Routes.ContainsKey(viewModelType) == false)
				return string.Empty;

			string routeSignature = Routes[viewModelType];
			return routeSignature;
		}
	}
}