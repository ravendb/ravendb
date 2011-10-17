// -----------------------------------------------------------------------
//  <copyright file="ModelAttacher.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Windows;

namespace Raven.Studio.Infrastructure
{
	public static class ModelAttacher
	{
		public static readonly DependencyProperty AttachModelProperty =
			DependencyProperty.RegisterAttached("AttachModel", typeof(string), typeof(ModelAttacher), new PropertyMetadata(null, AttachModelCallback));

		public static readonly DependencyProperty AttachObservableModelProperty =
			DependencyProperty.RegisterAttached("AttachObservableModel", typeof(string), typeof(ModelAttacher), new PropertyMetadata(null, AttachObservableModelCallback));

		private static void AttachModelCallback(DependencyObject source, DependencyPropertyChangedEventArgs args)
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
				view.DataContext = model;
			}
			catch (Exception ex)
			{
				throw new InvalidOperationException(string.Format("Cannot create instance of model type: {0}", modelType), ex);
			}
		}

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
			}
			catch (Exception ex)
			{
				throw new InvalidOperationException(string.Format("Cannot create instance of model type: {0}", modelType), ex);
			}
		}

		public static string GetAttachModel(UIElement element)
		{
			return (string)element.GetValue(AttachModelProperty);
		}

		public static void SetAttachModel(UIElement element, string value)
		{
			element.SetValue(AttachModelProperty, value);
		}

		public static string GetAttachObservableModel(UIElement element)
		{
			return (string)element.GetValue(AttachModelProperty);
		}

		public static void SetAttachObservableModel(UIElement element, string value)
		{
			element.SetValue(AttachModelProperty, value);
		}
	}
}