// Copyright (C) Microsoft Corporation. All Rights Reserved.
// This code released under the terms of the Microsoft Public License
// (Ms-PL, http://opensource.org/licenses/ms-pl.html).

// Modified by Samuel Jack for Hibernating Rhinos LTD
using System;
using System.Collections.ObjectModel;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using Boogaart.Silverlight.Behaviors;
using Popup = System.Windows.Controls.Primitives.Popup;

namespace Delay
{
    /// <summary>
    /// Implements a "split button" for Silverlight and WPF showing a popup instead of a context menu.
    /// </summary>
    [TemplatePart(Name = SplitElementName, Type = typeof(UIElement))]
    public class SplitButtonPopup : Button
    {
        public static readonly DependencyProperty PopupContentProperty =
            DependencyProperty.Register("PopupContent", typeof (object), typeof (SplitButtonPopup), new PropertyMetadata(default(object)));

        public static readonly DependencyProperty PopupAlignmentProperty =
            DependencyProperty.Register("PopupAlignment", typeof (PopupHorizontalAlignment), typeof (SplitButtonPopup), new PropertyMetadata(PopupHorizontalAlignment.Left));

        public PopupHorizontalAlignment PopupAlignment
        {
            get { return (PopupHorizontalAlignment) GetValue(PopupAlignmentProperty); }
            set { SetValue(PopupAlignmentProperty, value); }
        }

        public object PopupContent
        {
            get { return (object) GetValue(PopupContentProperty); }
            set { SetValue(PopupContentProperty, value); }
        }

        public static readonly DependencyProperty DropDownToolTipProperty =
            DependencyProperty.Register("DropDownToolTip", typeof(object), typeof(SplitButtonPopup), new PropertyMetadata(default(object)));

        public object DropDownToolTip
        {
            get { return (object) GetValue(DropDownToolTipProperty); }
            set { SetValue(DropDownToolTipProperty, value); }
        }

        /// <summary>
        /// Stores the public name of the split element.
        /// </summary>
        private const string SplitElementName = "SplitElement";
        private const string PopupElementName = "PopupElement";
        /// <summary>
        /// Stores a reference to the split element.
        /// </summary>
        private UIElement _splitElement;

        /// <summary>
        /// Stores a reference to the Popup.
        /// </summary>
        private Popup _popup;

        /// <summary>
        /// Gets or sets a value indicating whether the mouse is over the split element.
        /// </summary>
        protected bool IsMouseOverSplitElement { get; private set; }

        /// <summary>
        /// Initializes a new instance of the SplitButtonPopup class.
        /// </summary>
        public SplitButtonPopup()
        {
            DefaultStyleKey = typeof(SplitButtonPopup);
        }

        /// <summary>
        /// Called when the template is changed.
        /// </summary>
        public override void OnApplyTemplate()
        {
            // Unhook existing handlers
            if (null != _splitElement)
            {
                _splitElement.MouseEnter -= new MouseEventHandler(SplitElement_MouseEnter);
                _splitElement.MouseLeave -= new MouseEventHandler(SplitElement_MouseLeave);
                _splitElement = null;
            }
            if (null != _popup)
            {
                _popup.Opened -= Popup_Opened;
                _popup.Closed -= Popup_Closed;
                _popup = null;
            }

            // Apply new template
            base.OnApplyTemplate();

            // Hook new event handlers
            _splitElement = GetTemplateChild(SplitElementName) as UIElement;
            if (null != _splitElement)
            {
                _splitElement.MouseEnter += new MouseEventHandler(SplitElement_MouseEnter);
                _splitElement.MouseLeave += new MouseEventHandler(SplitElement_MouseLeave);

                _popup = GetTemplateChild(PopupElementName) as Popup;
                if (null != _popup)
                {
                    _popup.Opened += (Popup_Opened);
                    _popup.Closed += (Popup_Closed);
                }
            }
        }

        /// <summary>
        /// Called when the Button is clicked.
        /// </summary>
        protected override void OnClick()
        {
            if (IsMouseOverSplitElement)
            {
                OpenButtonMenu();
            }
            else
            {
                base.OnClick();
            }
        }

        /// <summary>
        /// Called when a key is pressed.
        /// </summary>
        protected override void OnKeyDown(KeyEventArgs e)
        {
            if (null == e)
            {
                throw new ArgumentNullException("e");
            }

            if ((Key.Down == e.Key) || (Key.Up == e.Key))
            {
                // WPF requires this to happen via BeginInvoke
                Dispatcher.BeginInvoke((Action)(() => OpenButtonMenu()));
            }
            else
            {
                base.OnKeyDown(e);
            }
        }

        /// <summary>
        /// Opens the button menu.
        /// </summary>
        protected void OpenButtonMenu()
        {
            if (null != _popup)
            {
                _popup.IsOpen = true;
            }
        }

        /// <summary>
        /// Called when the mouse goes over the split element.
        /// </summary>
        /// <param name="sender">Event source.</param>
        /// <param name="e">Event arguments.</param>
        private void SplitElement_MouseEnter(object sender, MouseEventArgs e)
        {
            IsMouseOverSplitElement = true;
        }

        /// <summary>
        /// Called when the mouse goes off the split element.
        /// </summary>
        /// <param name="sender">Event source.</param>
        /// <param name="e">Event arguments.</param>
        private void SplitElement_MouseLeave(object sender, MouseEventArgs e)
        {
            IsMouseOverSplitElement = false;
        }

        /// <summary>
        /// Called when the Popup is opened.
        /// </summary>
        /// <param name="sender">Event source.</param>
        /// <param name="e">Event arguments.</param>
        private void Popup_Opened(object sender, EventArgs e)
        {
            _popup.UpdateLayout();

            Dispatcher.BeginInvoke(UpdatePopupOffsets);
        }

        /// <summary>
        /// Called when the Popup is closed.
        /// </summary>
        /// <param name="sender">Event source.</param>
        /// <param name="e">Event arguments.</param>
        private void Popup_Closed(object sender, EventArgs e)
        {
        }

        /// <summary>
        /// Updates the Popup's Horizontal/VerticalOffset properties to keep it under the SplitButton.
        /// </summary>
        private void UpdatePopupOffsets()
        {
            if (PopupAlignment == PopupHorizontalAlignment.Right)
            {
                _popup.HorizontalOffset = -_popup.Child.DesiredSize.Width + ActualWidth;
            }
        }

        public bool IsPopupOpen
        {
            get
            {
                return _popup != null && _popup.IsOpen;
            }
            set
            {
                if (_popup != null)
                {
                    _popup.IsOpen = value;
                }
            }
        }
    }
}
