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
				var model = Activator.CreateInstance(modelType);
				var observableType = typeof(Observable<>).MakeGenericType(modelType);
				var observable = Activator.CreateInstance(observableType);
				var piValue = observableType.GetProperty("Value");
				piValue.SetValue(observable, model, null);
				view.DataContext = observable;

				var modelModel = model as Model;
				if (modelModel == null)	return;
				modelModel.ForceTimerTicked();

				SetPageTitle(modelType, model, view);

				view.Loaded += (sender, eventArgs) =>
				{
					var viewModel = model as ViewModel;
					if (viewModel == null) return;
					viewModel.LoadModel(UrlUtil.Url);
				};
			}
			catch (Exception ex)
			{
				throw new InvalidOperationException(string.Format("Cannot create instance of model type: {0}", modelType), ex);
			}
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