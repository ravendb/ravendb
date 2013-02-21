using System;
using System.Linq;
using System.Net;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Studio.Infrastructure.Converters;

namespace Raven.Studio.Features.Documents
{
    public static class ColumnModelBindingExtensions
    {
        private static string ExpandBinding(this string binding)
        {
            if (binding.StartsWith("$JsonDocument:"))
            {
                return binding.Substring("$JsonDocument:".Length);
            }
            if (binding.StartsWith("$Meta:"))
            {
                return "Metadata" + ExpandPropertyPathToXamlBinding(binding.Substring("$Meta:".Length));
            }
            if (binding == "$Temp:Score")
            {
                return "TempIndexScore";
            }
	        return "DataAsJson" + ExpandPropertyPathToXamlBinding(binding);
        }

        private static string ExpandPropertyPathToXamlBinding(string binding)
        {
            // for example TestNested[0].MyProperty will be expanded to [TestNested][0][MyProperty]
            var result = binding.Split(new[] { '.' }, StringSplitOptions.RemoveEmptyEntries)
                .SelectMany(s => s.Split(new[] { '[', ']' }, StringSplitOptions.RemoveEmptyEntries))
                .Aggregate(new StringBuilder(), (sb, next) =>
                {
                    sb.Append('[');
                    sb.Append(next);
                    sb.Append(']');
                    return sb;
                }, sb => sb.ToString());

            return result;
        }

        public static string GetBindingPath(this ColumnDefinition columnDefinition, string pathPrefix)
        {
            return pathPrefix + ExpandBinding(columnDefinition.Binding);
        }

        public static Binding CreateBinding(this ColumnDefinition columnDefinition, string pathPrefix)
        {
            return new Binding(GetBindingPath(columnDefinition, pathPrefix)) { Converter = DocumentPropertyToSingleLineStringConverter.Default };
        }
    }
}
