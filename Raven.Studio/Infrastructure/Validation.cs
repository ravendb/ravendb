using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Abstractions.Extensions;

namespace Raven.Studio.Infrastructure
{
    public static class Validation
    {
        public static bool Validate(object instance, ICollection<ValidationResult> validationResults, Action<string> onErrorsChanged)
        {
            var propertiesThatHadErrors = validationResults.SelectMany(v => v.MemberNames).Distinct().ToHashSet();

            validationResults.Clear();

            var validationResult = Validator.TryValidateObject(instance, new ValidationContext(instance),
                                                               validationResults, true);

            var propertiesThatHaveErrors = validationResults.SelectMany(v => v.MemberNames).Distinct().ToHashSet();

            foreach (var property in propertiesThatHaveErrors.Concat(propertiesThatHadErrors).Distinct())
            {
                onErrorsChanged(property);
            }

            return validationResult;
        }
    }
}
