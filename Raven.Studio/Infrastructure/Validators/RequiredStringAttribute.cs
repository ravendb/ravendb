using System.ComponentModel.DataAnnotations;

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
            return !string.IsNullOrEmpty(stringValue) 
                ? ValidationResult.Success : new ValidationResult("A string value is required", new[] { validationContext.MemberName });
        }
    }
}