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
using Microsoft.Expression.Interactivity;

namespace Raven.Studio.Behaviors
{
    public class GoToStateAction : TargetedTriggerAction<FrameworkElement>
    {
        public static readonly DependencyProperty UseTransitionsProperty =
            DependencyProperty.Register("UseTransitions", typeof(bool), typeof(GoToStateAction), new PropertyMetadata(true));

        public static readonly DependencyProperty StateNameProperty =
            DependencyProperty.Register("StateName", typeof(string), typeof(GoToStateAction), new PropertyMetadata(null));

        private FrameworkElement _stateTarget;

        public string StateName
        {
            get { return (string)GetValue(StateNameProperty); }
            set { SetValue(StateNameProperty, value); }
        }

        public bool UseTransitions
        {
            get { return (bool)GetValue(UseTransitionsProperty); }
            set { SetValue(UseTransitionsProperty, value); }
        }

        private bool IsTargetObjectSet
        {
            get { return ReadLocalValue(TargetObjectProperty) != DependencyProperty.UnsetValue; }
        }

        protected override void OnTargetChanged(FrameworkElement oldTarget, FrameworkElement newTarget)
        {
            base.OnTargetChanged(oldTarget, newTarget);

            FrameworkElement target;

            if (string.IsNullOrEmpty(TargetName) && !IsTargetObjectSet)
            {
                VisualStateUtilities.TryFindNearestStatefulControl(AssociatedObject as FrameworkElement, out target);
            }
            else
            {
                target = Target;
            }

            _stateTarget = target;
        }

        protected override void Invoke(object parameter)
        {
            if (_stateTarget == null || StateName == null)
            {
                return;
            }

            VisualStateUtilities.GoToState(_stateTarget, StateName, UseTransitions);
        }
    }
}
