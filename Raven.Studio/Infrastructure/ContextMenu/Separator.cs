// (c) Copyright Microsoft Corporation.
// This source is subject to the Microsoft Public License (Ms-PL).
// Please see http://go.microsoft.com/fwlink/?LinkID=131993 for details.
// All other rights reserved.

using System.Windows.Controls;

namespace Raven.Studio.Infrastructure.ContextMenu
{
    /// <summary>
    /// Control that is used to separate items in items controls.
    /// </summary>
    /// <QualityBand>Preview</QualityBand>
    public class Separator : Control
    {
        /// <summary>
        /// Initializes a new instance of the Separator class.
        /// </summary>
        public Separator()
        {
            DefaultStyleKey = typeof(Separator);
        }
    }
}
