// -----------------------------------------------------------------------
//  <copyright file="ModelAttacher.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Windows;
using System.Windows.Controls;
using Raven.Studio.Models;

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
				var model = (Model)Activator.CreateInstance(modelType);
				model.ForceTimerTicked();

				var observable = (IObservable)Activator.CreateInstance(typeof(Observable<>).MakeGenericType(modelType));
				observable.Value = model;
				view.DataContext = observable;

				SetPageTitle(model, view);
			}
			catch (Exception ex)
			{
				throw new InvalidOperationException(string.Format("Cannot create instance of model type: {0}", modelType), ex);
			}
		}

		private static void SetPageTitle(Model model, FrameworkElement view)
		{
			var hasPageTitle = model as IHasPageTitle;
			if (hasPageTitle == null)
				return;

			var page = view as Page;
			if (page == null)
				return;
			page.Title = hasPageTitle.PageTitle;
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