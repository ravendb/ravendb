using System;
using System.ComponentModel.DataAnnotations;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;

namespace Raven.Studio.Infrastructure.Validators
{
    public class RequiredStringAttribute : ValidationAttribute
    {
        protected override ValidationResult IsValid(object value, ValidationContext validationContext)
        {
            if ((validationContext.ObjectInstance is IValidationSuppressible) && ((IValidationSuppressible)validationContext.ObjectInstance).SuppressValidation)
            {
                return ValidationResult.Success;
            }

            var stringValue = (value as string);
            if (!string.IsNullOrEmpty(stringValue))
            {
                return ValidationResult.Success;
            }
            else
            {
                return new ValidationResult("A string value is required", new[] { validationContext.MemberName });
            }
        }
    }
}
