namespace Raven.Studio.Framework {
    using System.Windows;
    using Caliburn.Micro;

    public class CustomActionMessage : ActionMessage {
        public static readonly DependencyProperty TargetProperty = DependencyProperty.Register(
            "Target",
            typeof(object),
            typeof(CustomActionMessage),
            new PropertyMetadata(null)
            );

        public object Target {
            get { return GetValue(TargetProperty); }
            set { SetValue(TargetProperty, value); }
        }

        public bool IgnoreAvailability { get; set; }

        static CustomActionMessage() {
            var originalSetBinding = SetMethodBinding;
            SetMethodBinding = context => {
                var custom = context.Message as CustomActionMessage;
                if(custom != null && custom.Target != null) {
                    var key = custom.Target as string;
                    if(key != null) {
                        context.Target = IoC.GetInstance(typeof(object), key);
                        context.Method = GetTargetMethod(context.Message, context.Target);
                    }
                    else {
                        context.Target = custom.Target;
                        context.Method = GetTargetMethod(context.Message, context.Target);
                    }
                }
                else {
                    originalSetBinding(context);
                }
            };

            var originalApplyAvailabilityEffect = ApplyAvailabilityEffect;
            ApplyAvailabilityEffect = context => {
                var custom = context.Message as CustomActionMessage;
                if(custom != null && custom.IgnoreAvailability)
                    return true;
                return originalApplyAvailabilityEffect(context);
            };
        }
    }
}