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
    public class DataGridLengthAttribute : ValidationAttribute
    {
        private static DataGridLengthConverter ConverterInstance = new DataGridLengthConverter();

        protected override ValidationResult IsValid(object value, ValidationContext validationContext)
        {
            var stringValue = (value as string);
            if (string.IsNullOrEmpty(stringValue))
            {
                return ValidationResult.Success;
            }

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
