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

namespace Raven.Studio.Features.Documents
{
    public class DocumentDetailsColumnGenerator : DataGridColumnGenerator
    {
        protected override string GetXamlForDataTemplate(ColumnDefinition definition)
        {
            var templateString =
               @"<DataTemplate  xmlns=""http://schemas.microsoft.com/winfx/2006/xaml/presentation"" xmlns:Behaviors=""clr-namespace:Raven.Studio.Behaviors;assembly=Raven.Studio""
 xmlns:m=""clr-namespace:Raven.Studio.Infrastructure.MarkupExtensions;assembly=Raven.Studio"" xmlns:i=""http://schemas.microsoft.com/expression/2010/interactivity""
                                    xmlns:Converters=""clr-namespace:Raven.Studio.Infrastructure.Converters;assembly=Raven.Studio"">
                                    <TextBlock Text=""{Binding $$$BindingPath$$$, Converter={m:Static Member=Converters:DocumentPropertyToSingleLineStringConverter.Trimmed}}""
                                               Behaviors:FadeTrimming.IsEnabled=""True"" Behaviors:FadeTrimming.ShowTextInToolTipWhenTrimmed=""True""
                                               VerticalAlignment=""Center""
                                               Margin=""5,0"">
                                        <i:Interaction.Behaviors>
                                            <Behaviors:ShowQuickDocumentPopupBehavior PotentialDocumentId=""{Binding $$$BindingPath$$$}""/>
                                        </i:Interaction.Behaviors>
                                    </TextBlock>
                                </DataTemplate>";

            templateString = templateString.Replace("$$$BindingPath$$$", definition.GetBindingPath("Item.Document."));

            return templateString;
        }

        protected override Binding GetBinding(ColumnDefinition definition)
        {
            return definition.CreateBinding("Item.Document.");
        }
    }
}
