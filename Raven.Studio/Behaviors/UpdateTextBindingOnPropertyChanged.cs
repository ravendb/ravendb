// -----------------------------------------------------------------------
//  <copyright file="UpdateTextBindingOnPropertyChanged.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Windows.Controls;
using System.Windows.Data;

namespace Raven.Studio.Behaviors
{
	public class UpdateTextBindingOnPropertyChanged : StudioBehavior<TextBox>
	{
		private BindingExpression expression;

		protected override void OnAttached()
		{
			base.OnAttached();

			expression = AssociatedObject.GetBindingExpression(TextBox.TextProperty);
			AssociatedObject.TextChanged += this.OnTextChanged;
		}

		protected override void OnDetaching()
		{
			base.OnDetaching();

			AssociatedObject.TextChanged -= this.OnTextChanged;
			expression = null;
		}

		private void OnTextChanged(object sender, EventArgs args)
		{
			expression.UpdateSource();
		}
	}
}