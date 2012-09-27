using System.ComponentModel.DataAnnotations;
using System.Windows.Controls;

namespace Raven.Studio.Infrastructure.Validators
{
    public class DataGridLengthAttribute : ValidationAttribute
    {
        private static DataGridLengthConverter ConverterInstance = new DataGridLengthConverter();

        protected override ValidationResult IsValid(object value, ValidationContext validationContext)
        {
            var stringValue = (value as string);
            if (string.IsNullOrEmpty(stringValue))
                return ValidationResult.Success;

            try
            {
                ConverterInstance.ConvertFromString(stringValue);
                return ValidationResult.Success;
            }
            catch
            {
                return new ValidationResult("Examples of valid values: 'Auto', '150', '2*'", new[] {validationContext.MemberName});
            }
        }
    }
}