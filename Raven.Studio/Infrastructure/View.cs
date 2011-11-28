using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Threading;

namespace Raven.Studio.Infrastructure
{
	public class View : Page
	{
		public static List<View> CurrentViews { get; set; }

		private static readonly DispatcherTimer dispatcherTimer;

		static View()
		{
			CurrentViews = new List<View>();
			dispatcherTimer = new DispatcherTimer
			{
				Interval = TimeSpan.FromSeconds(1),
			};
			dispatcherTimer.Tick += DispatcherTimerOnTick;
			dispatcherTimer.Start();			
		}

		private static void DispatcherTimerOnTick(object sender, EventArgs eventArgs)
		{
			foreach (var ctx in CurrentViews.Select(view => view.DataContext).Distinct())
			{
				InvokeTimerTicked(ctx);
			}
		}

		public static void UpdateAllFromServer()
		{
			foreach (var ctx in CurrentViews.Select(view => view.DataContext).Distinct())
			{
				InvokeOnModel(ctx, model => model.ForceTimerTicked());
			}
		}

		private static void InvokeTimerTicked(object ctx)
		{
			InvokeOnModel(ctx, model => model.TimerTicked());
		}

		private static void InvokeOnModel(object ctx, Action<Model> action)
		{
			var model = ctx as Model;
			if (model == null)
			{
				var observable = ctx as IObservable;
				if (observable == null)
					return;
				model = observable.Value as Model;
				if (model == null)
				{
					PropertyChangedEventHandler observableOnPropertyChanged = null;
					observableOnPropertyChanged = (sender, args) =>
					                              {
					                              	if (args.PropertyName != "Value")
					                              		return;
					                              	observable.PropertyChanged -= observableOnPropertyChanged;
					                              	InvokeOnModel(ctx, action);
					                              };
					observable.PropertyChanged += observableOnPropertyChanged;
				}
			}
			action(model);
		}


		// Dependency property that is bound against the DataContext.
		// When its value (i.e. the control's DataContext) changes,
		// call DataContextWatcher_Changed.
		public static DependencyProperty DataContextWatcherProperty = DependencyProperty.Register(
		  "DataContextWatcher",
		  typeof(object),
		  typeof(View),
			  new PropertyMetadata(DataContextWatcherChanged));

		private static void DataContextWatcherChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
		{
			InvokeTimerTicked(e.NewValue);
		}

		public View()
		{
			SetBinding(DataContextWatcherProperty, new Binding());

			Loaded += (sender, args) => CurrentViews.Add(this);

			Unloaded += (sender, args) => CurrentViews.Remove(this);
		}
	}
}