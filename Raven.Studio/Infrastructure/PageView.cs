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
	public class PageView : Page
	{
		public static List<PageView> CurrentViews { get; set; }

		private static readonly DispatcherTimer dispatcherTimer;

		static PageView()
		{
			CurrentViews = new List<PageView>();
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

		public static List<Model> UpdateAllFromServer()
		{
			var result = new List<Model>();
			foreach (var ctx in CurrentViews.Select(view => view.DataContext).Distinct())
			{
				var x = InvokeOnModel(ctx, model => model.ForceTimerTicked());
				if(x != null)
					result.Add(x);
			}
			return result;
		}

		private static void InvokeTimerTicked(object ctx)
		{
			InvokeOnModel(ctx, model => model.TimerTicked());
		}

		private static Model InvokeOnModel(object ctx, Action<Model> action)
		{
			var model = ctx as Model;
			if (model == null)
			{
				var observable = ctx as IObservable;
				if (observable == null)
					return null;
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
					return null;
				}
			}
			action(model);
			return model;
		}


		// Dependency property that is bound against the DataContext.
		// When its value (i.e. the control's DataContext) changes,
		// call DataContextWatcher_Changed.
		public static DependencyProperty DataContextWatcherProperty = DependencyProperty.Register(
			"DataContextWatcher",
			typeof (object),
			typeof (PageView),
			new PropertyMetadata(DataContextWatcherChanged));

		private static void DataContextWatcherChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
		{
			InvokeTimerTicked(e.NewValue);
		}

		public PageView()
		{
			SetBinding(DataContextWatcherProperty, new Binding());

			Loaded += (sender, args) => CurrentViews.Add(this);

			Unloaded += (sender, args) => CurrentViews.Remove(this);
		}

        protected override void OnNavigatedTo(System.Windows.Navigation.NavigationEventArgs e)
        {
            base.OnNavigatedTo(e);

            if (DataContext is IObservable && (DataContext as IObservable).Value is PageViewModel)
            {
                (((DataContext as IObservable).Value) as PageViewModel).LoadModel(UrlUtil.Url);
            }
        }
	}
}