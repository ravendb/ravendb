using System.Windows;

namespace Raven.Studio.Infrastructure.Converters
{
    public class TargetedDataTemplate
    {
        public string TargetType { get; set; }

        public DataTemplate Template { get; set; }
    }
}