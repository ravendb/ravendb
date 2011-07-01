// -----------------------------------------------------------------------
//  <copyright file="CanGetStats.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.ComponentModel.Composition;
using System.Windows;
using System.Windows.Interop;
using Caliburn.Micro;
using Raven.Studio.Messages;

namespace Raven.Studio.Infrastructure.Services
{
	[Export]
	public class NavigationService
	{
		readonly IEventAggregator events;

		[ImportingConstructor]
		public NavigationService(IEventAggregator events)
		{
			this.events = events;
		}

		public void Initialize()
		{
			Application.Current.Host.NavigationStateChanged += (sender, args) =>
			                                                   	{
			                                                   		Navigate(args);
			                                                   	};
		}

		private void Navigate(NavigationStateChangedEventArgs e)
		{
			var uriBinder = new UriBinder();
			var viewModelResolved = uriBinder.ResolveViewModel(e.NewNavigationState);
			events.Publish(new NavigationOccurred2(viewModelResolved.Item1, viewModelResolved.Item2));
		}
	}
}