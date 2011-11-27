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
using System.Windows.Data;
using System.Globalization;
using System.Collections.ObjectModel;
using System.Collections.Generic;
using System.Collections;

namespace Kieners.Silverlight
{

    public class RelativeSourceBinding : RelativeSourceBase
    {

        /// <summary>
        /// Gets or sets the path to the binding source property.
        /// </summary>
        public string Path { get; set; }

        /// <summary>
        /// Gets or sets the name of the target dependency property.
        /// </summary>
        public string TargetProperty { get; set; }

        /// <summary>
        /// Gets or sets the XAML namespace. This namespace is used to get the class of an attached dependency property.
        /// </summary>
        public string TargetNamespace { get; set; }

        /// <summary>
        /// Gets or sets the type of ancestor to look for. 
        /// Define the full name (namespace and class name). Xaml Namespace do not work here.
        /// Example: MyNamespace.MyUserControl or System.Windows.ListBox.
        /// For types in System.Windows.dll you can just use the class name instead of the full name.
        /// </summary>
        public string AncestorType { get; set; }

        // not implemented yet
        //public int AncestorLevel { get; set; }

        /// <summary>
        ///  Gets or sets a value that describes the location of the binding source relative
        ///  to the position of the binding target.
        /// </summary>
        public RelativeSourceMode RelativeMode { get; set; }

        /// <summary>
        ///  Gets or sets a value that indicates the direction of the data flow in the binding.
        /// </summary>
        public BindingMode BindingMode { get; set; }

        /// <summary>
        /// Gets or sets the converter object that is called by the binding engine to 
        /// modify the data as it is passed between the source and target, or vice versa.
        /// </summary>
        public IValueConverter Converter { get; set; }

        /// <summary>
        /// Gets or sets the culture to be used by the System.Windows.Data.Binding.Converter.
        /// </summary>
        public CultureInfo ConverterCulture { get; set; }

        /// <summary>
        /// Gets or sets a parameter that can be used in the System.Windows.Data.Binding.Converter logic.
        /// </summary>
        public object ConverterParameter { get; set; }

        /// <summary>
        /// Gets or sets a value that indicates whether the System.Windows.FrameworkElement.BindingValidationError event is raised on validation errors.
        /// </summary>
        public bool NotifyOnValidationError { get; set; }

        /// <summary>
        /// Gets or sets a value that indicates whether the binding engine will report
        /// validation errors from an System.ComponentModel.IDataErrorInfo implementation
        /// on the bound data entity.
        /// </summary>
        public bool ValidatesOnDataErrors { get; set; }

        /// <summary>
        /// Gets or sets a value that indicates whether the binding engine will report 
        /// exception validation errors.
        /// </summary>
        public bool ValidatesOnExceptions { get; set; }

        /// <summary>
        /// Gets or sets a value that indicates whether the binding engine will report
        /// validation errors from an System.ComponentModel.INotifyDataErrorInfo implementation
        /// on the bound data entity.
        /// </summary>
        public bool ValidatesOnNotifyDataErrors { get; set; }


    }

}
