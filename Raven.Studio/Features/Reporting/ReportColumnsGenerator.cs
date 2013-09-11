using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Studio.Behaviors;
using ColumnDefinition = Raven.Studio.Features.Documents.ColumnDefinition;

namespace Raven.Studio.Features.Reporting
{
    public class ReportColumnsGenerator : DataGridColumnGenerator
    {
        protected override string GetXamlForDataTemplate(ColumnDefinition definition)
        {
            var templateString =
               @"<DataTemplate  xmlns=""http://schemas.microsoft.com/winfx/2006/xaml/presentation"" xmlns:Behaviors=""clr-namespace:Raven.Studio.Behaviors;assembly=Raven.Studio""
 xmlns:m=""clr-namespace:Raven.Studio.Infrastructure.MarkupExtensions;assembly=Raven.Studio""
                                    xmlns:Converters=""clr-namespace:Raven.Studio.Infrastructure.Converters;assembly=Raven.Studio"">
                                    <TextBlock Text=""{Binding $$$BindingPath$$$}""
                                               Behaviors:FadeTrimming.IsEnabled=""True"" Behaviors:FadeTrimming.ShowTextInToolTipWhenTrimmed=""True""
                                               VerticalAlignment=""Center""
                                               Margin=""5,2""/>
                                </DataTemplate>";

            templateString = templateString.Replace("$$$BindingPath$$$", definition.Binding);

            return templateString;
        }

        protected override Binding GetBinding(ColumnDefinition definition)
        {
            return new Binding(definition.Binding);
        }
    }
}
