// (c) Copyright Microsoft Corporation.
// This source is subject to the Microsoft Public License (Ms-PL).
// Please see http://go.microsoft.com/fwlink/?LinkID=131993 for details.
// All other rights reserved.

using System.Windows;

namespace Raven.Studio.Infrastructure.ContextMenu
{
    /// <summary>
    /// Provides the system implementation for displaying a ContextMenu.
    /// </summary>
    /// <QualityBand>Preview</QualityBand>
    public static class ContextMenuService
    {
        /// <summary>
        /// Gets the value of the ContextMenu property of the specified object.
        /// </summary>
        /// <param name="obj">Object to query concerning the ContextMenu property.</param>
        /// <returns>Value of the ContextMenu property.</returns>
        public static ContextMenu GetContextMenu(DependencyObject obj)
        {
            return (ContextMenu)obj.GetValue(ContextMenuProperty);
        }

        /// <summary>
        /// Sets the value of the ContextMenu property of the specified object.
        /// </summary>
        /// <param name="obj">Object to set the property on.</param>
        /// <param name="value">Value to set.</param>
        public static void SetContextMenu(DependencyObject obj, ContextMenu value)
        {
            obj.SetValue(ContextMenuProperty, value);
        }

        /// <summary>
        /// Identifies the ContextMenu attached property.
        /// </summary>
        public static readonly DependencyProperty ContextMenuProperty = DependencyProperty.RegisterAttached(
            "ContextMenu",
            typeof(ContextMenu),
            typeof(ContextMenuService),
            new PropertyMetadata(null, OnContextMenuChanged));

        /// <summary>
        /// Handles changes to the ContextMenu DependencyProperty.
        /// </summary>
        /// <param name="o">DependencyObject that changed.</param>
        /// <param name="e">Event data for the DependencyPropertyChangedEvent.</param>
        private static void OnContextMenuChanged(DependencyObject o, DependencyPropertyChangedEventArgs e)
        {
            FrameworkElement element = o as FrameworkElement;
            if (null != element)
            {
                ContextMenu oldContextMenu = e.OldValue as ContextMenu;
                if (null != oldContextMenu)
                {
                    oldContextMenu.Owner = null;
                }
                ContextMenu newContextMenu = e.NewValue as ContextMenu;
                if (null != newContextMenu)
                {
                    newContextMenu.Owner = element;
                }
            }
        }
    }
}
