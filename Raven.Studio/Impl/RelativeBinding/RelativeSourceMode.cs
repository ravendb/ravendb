using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;

namespace Kieners.Silverlight
{
    /// <summary>
    /// Describes the location of the binding source relative to the position of the binding target.
    /// </summary>
    public enum RelativeSourceMode
    {
        /// <summary>
        /// Refers to the next parent data context of the data-bound element. You 
        /// can use this to bind to an data context up the chain.
        /// </summary>
        ParentDataContext = 0,
       
        /// <summary>
        /// Refers to the ancestor in the parent chain of the data-bound element. You 
        /// can use this to bind to an ancestor of a specific type or its subclasses.
        /// This is the mode you use if you want to specify AncestorType.
        /// </summary>
        FindAncestor = 1

    }
}
