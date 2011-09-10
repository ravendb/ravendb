using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Controls;

namespace Raven.Studio.Infrastructure
{
	public abstract class View : Page
	{
		public static List<View> CurrentViews { get; set; }

		private static Timer _timer = new Timer(TimerCallback, null, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10));

		static View()
		{
			CurrentViews = new List<View>();
		}

		private static void TimerCallback(object state)
		{
			View[] views;
			lock (CurrentViews)
			{
				views = CurrentViews.ToArray();
			}
			foreach (var ctx in views.Select(view => view.SafeForThreadingDataContext))
			{
				InvokeTimerTicked(ctx);
			}
		}

		private static void InvokeTimerTicked(object ctx)
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
						InvokeTimerTicked(ctx);
					};
					observable.PropertyChanged += observableOnPropertyChanged;
					return;
				}
			}

			model.TimerTicked();
		}

		private object SafeForThreadingDataContext { get; set; }

		protected View()
		{
			Loaded += (sender, args) =>
			{
				lock (CurrentViews)
					CurrentViews.Add(this);
				SafeForThreadingDataContext = DataContext;
				InvokeTimerTicked(SafeForThreadingDataContext);
			};

			Unloaded += (sender, args) =>
			{
				lock (CurrentViews)
					CurrentViews.Remove(this);
			};
		}
	}
}