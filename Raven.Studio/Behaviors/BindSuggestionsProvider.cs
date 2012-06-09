using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Interactivity;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Behaviors
{
    public class BindSuggestionsProvider : Behavior<AutoCompleteBox>
    {
        public static readonly DependencyProperty SuggestionProviderProperty =
            DependencyProperty.Register("SuggestionProvider", typeof (IAutoCompleteSuggestionProvider), typeof (BindSuggestionsProvider), new PropertyMetadata(default(IAutoCompleteSuggestionProvider)));

        public IAutoCompleteSuggestionProvider SuggestionProvider
        {
            get { return (IAutoCompleteSuggestionProvider) GetValue(SuggestionProviderProperty); }
            set { SetValue(SuggestionProviderProperty, value); }
        }

        protected override void OnAttached()
        {
            base.OnAttached();

            AssociatedObject.Populating += HandlePopulating;
        }

        protected override void OnDetaching()
        {
            base.OnDetaching();

            AssociatedObject.Populating -= HandlePopulating;
        }

        private void HandlePopulating(object sender, PopulatingEventArgs e)
        {
            e.Cancel = true;
            if (SuggestionProvider != null)
            {
                SuggestionProvider.ProvideSuggestions(e.Parameter)
                    .ContinueOnSuccessInTheUIThread(results =>
                                                        {
                                                            AssociatedObject.ItemsSource = results;
                                                            AssociatedObject.PopulateComplete();
                                                        });
            }
        }
    }
}
