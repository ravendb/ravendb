// Copyright (C) Microsoft Corporation. All Rights Reserved.
// This code released under the terms of the Microsoft Public License
// (Ms-PL, http://opensource.org/licenses/ms-pl.html).

namespace Delay
{
    /// <summary>
    /// Implements a "menu button" for Silverlight and WPF.
    /// </summary>
    public class MenuButton : SplitButton
    {
        /// <summary>
        /// Initializes a new instance of the MenuButton class.
        /// </summary>
        public MenuButton()
        {
            DefaultStyleKey = typeof(MenuButton);
        }

        /// <summary>
        /// Called when the button is clicked.
        /// </summary>
        protected override void OnClick()
        {
            OpenButtonMenu();
        }
    }
}
