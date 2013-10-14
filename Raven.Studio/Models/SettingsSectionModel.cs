using Raven.Abstractions.Data;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
    public abstract class SettingsSectionModel : ViewModel
    {
        public string SectionName { get; protected set; }

		public bool HasUnsavedChanges { get; set; }

        public virtual void LoadFor(DatabaseDocument document)
        {
        }

	    public virtual void CheckForChanges()
	    {		    
	    }

	    public abstract void MarkAsSaved();
    }
}