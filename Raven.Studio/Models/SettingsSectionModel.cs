using Raven.Abstractions.Data;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
    public class SettingsSectionModel : ViewModel
    {
        public string SectionName { get; protected set; }

        public virtual void LoadFor(DatabaseDocument document)
        {
        }
    }
}