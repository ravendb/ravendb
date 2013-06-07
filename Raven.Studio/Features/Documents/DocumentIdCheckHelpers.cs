using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Documents
{
    public static class DocumentIdCheckHelpers
    {
        public static bool IsPotentialId(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return false;
            }

            var pattern = @"^\w+[/-]\w+";
            return Regex.IsMatch(value, pattern);
        }

        public static Task<IList<string>> GetActualIds(IEnumerable<string> potentialIds)
        {
            return ApplicationModel.Current.Server.Value.SelectedDatabase.Value.AsyncDatabaseCommands.GetAsync(
                   potentialIds.ToArray(), null, metadataOnly: true)
                               .ContinueOnSuccess(results =>
                               {
                                   var ids =
                                       results.Results.Where(r => r != null).Select(
                                           r => r["@metadata"].SelectToken("@id").ToString()).ToList();

                                   return (IList<string>)ids;
                               });
        }
    }
}
