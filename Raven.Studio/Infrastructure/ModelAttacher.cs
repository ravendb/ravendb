// -----------------------------------------------------------------------
//  <copyright file="ModelAttacher.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Windows;
using System.Windows.Controls;

namespace Raven.Studio.Infrastructure
{
	public static class ModelAttacher
	{
		public static readonly DependencyProperty AttachObservableModelProperty =
			DependencyProperty.RegisterAttached("AttachObservableModel", typeof(string), typeof(ModelAttacher), new PropertyMetadata(null, AttachObservableModelCallback));
		
		private static void AttachObservableModelCallback(DependencyObject source, DependencyPropertyChangedEventArgs args)
		{
			var typeName = args.NewValue as string;
			var view = source as FrameworkElement;
			if (typeName == null || view == null)
				return;

			var modelType = Type.GetType("Raven.Studio.Models." + typeName) ?? Type.GetType(typeName);
			if (modelType == null)
				return;

			try
			{
				var modelInstance = Activator.CreateInstance(modelType);
				var observableType = typeof(Observable<>).MakeGenericType(modelType);
				var observable = Activator.CreateInstance(observableType) as IObservable;
				var piValue = observableType.GetProperty("Value");
				piValue.SetValue(observable, modelInstance, null);
				view.DataContext = observable;

				var model = modelInstance as Model;
				if (model == null) 
					return;
				model.ForceTimerTicked();

				SetPageTitle(modelType, modelInstance, view);
				
				var weakListener = new WeakEventListener<IObservable, object, RoutedEventArgs>(observable);
				view.Loaded += weakListener.OnEvent;
				weakListener.OnEventAction = OnViewLoaded;
				weakListener.OnDetachAction = listener => view.Loaded -= listener.OnEvent;
			}
			catch (Exception ex)
			{
				throw new InvalidOperationException(string.Format("Cannot create instance of model type: {0}", modelType), ex);
			}
		}

		private static void OnViewLoaded(IObservable observable, object sender, RoutedEventArgs arg)
		{
			var model = (Model)observable.Value;
			model.ForceTimerTicked();

			var viewModel = model as ViewModel;
			if (viewModel == null) return;
			viewModel.LoadModel(UrlUtil.Url);
		}

		private static void SetPageTitle(Type modelType, object model, FrameworkElement view)
		{
			var piTitle = modelType.GetProperty("ViewTitle");
			if (piTitle == null) return;
			var page = view as Page;
			if (page == null) return;
			page.Title = piTitle.GetValue(model, null) as string;
		}

		public static string GetAttachObservableModel(UIElement element)
		{
			return (string)element.GetValue(AttachObservableModelProperty);
		}

		public static void SetAttachObservableModel(UIElement element, string value)
		{
			element.SetValue(AttachObservableModelProperty, value);
		}
	}
}