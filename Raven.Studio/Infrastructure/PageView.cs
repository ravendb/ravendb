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
	    private bool isLoaded;

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

		public PageView()
		{
			Loaded += HandleLoaded;
		    Unloaded += HandleUnloaded;

		    DataContextChanged += HandleDataContectChanged;
		}

        protected override void OnNavigatedTo(System.Windows.Navigation.NavigationEventArgs e)
        {
            base.OnNavigatedTo(e);

            if (DataContext is IObservable && (DataContext as IObservable).Value is PageViewModel)
            {
                (((DataContext as IObservable).Value) as PageViewModel).LoadModel(UrlUtil.Url);
            }
        }

        protected override void OnNavigatingFrom(System.Windows.Navigation.NavigatingCancelEventArgs e)
        {
            base.OnNavigatingFrom(e);

            if (DataContext is IObservable && (DataContext as IObservable).Value is PageViewModel)
            {
                e.Cancel = !(((DataContext as IObservable).Value) as PageViewModel).CanLeavePage();
            }
        }

        private void HandleDataContectChanged(object sender, DependencyPropertyChangedEventArgs e)
        {
            if (isLoaded)
            {
                var oldViewModel = e.OldValue as ViewModel;
                if (oldViewModel != null)
                {
                    oldViewModel.NotifyViewUnloaded();
                }

                var newViewModel = e.NewValue as ViewModel;
                if (newViewModel != null)
                {
                    newViewModel.NotifyViewLoaded();
                }

                InvokeTimerTicked(e.NewValue);
            }
        }

        private void HandleLoaded(object sender, RoutedEventArgs e)
        {
            CurrentViews.Add(this);

            InvokeOnModel(DataContext, m => { if (m is ViewModel) (m as ViewModel).NotifyViewLoaded(); });

            isLoaded = true;
        }

        private void HandleUnloaded(object sender, RoutedEventArgs e)
        {
            CurrentViews.Remove(this);

            InvokeOnModel(DataContext, m => { if (m is ViewModel) (m as ViewModel).NotifyViewUnloaded(); });

            isLoaded = false;
        }
	}
}