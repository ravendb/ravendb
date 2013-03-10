// -----------------------------------------------------------------------
//  <copyright file="TextBoxUpdateBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Windows.Controls;
using System.Windows.Interactivity;

namespace Raven.Studio.Behaviors
{
	public class TextBoxUpdateBehavior : Behavior<TextBox>
	{
		protected override void OnAttached()
		{
			AssociatedObject.TextChanged += TextBoxTextChanged;
		}

		protected override void OnDetaching()
		{
			AssociatedObject.TextChanged -= TextBoxTextChanged;
		}

		private void TextBoxTextChanged(object sender, TextChangedEventArgs e)
		{
			var binding = AssociatedObject.GetBindingExpression(TextBox.TextProperty);
			if (binding != null) 
				binding.UpdateSource();
		}
	}
}