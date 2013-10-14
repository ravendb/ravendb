using Raven.Abstractions.Data;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Settings
{
    public class QuotaSettingsSectionModel : SettingsSectionModel
    {
        public QuotaSettingsSectionModel()
        {
            SectionName = "Quotas";
        }

		public long MaxSize { get; set; }
		public long WarnSize { get; set; }
		public long MaxDocs { get; set; }
		public long WarnDocs { get; set; }

		public long OriginalMaxSize { get; set; }
		public long OriginalWarnSize { get; set; }
		public long OriginalMaxDocs { get; set; }
		public long OriginalWarnDocs { get; set; }

        public override void LoadFor(DatabaseDocument document)
        {
			MaxSize = ReadSettingAsInt(document, Constants.SizeHardLimitInKB) / 1024;
			WarnSize = ReadSettingAsInt(document, Constants.SizeSoftLimitInKB) / 1024;
            MaxDocs = ReadSettingAsInt(document, Constants.DocsHardLimit);
            WarnDocs = ReadSettingAsInt(document, Constants.DocsSoftLimit);

	        OriginalMaxSize = MaxSize;
	        OriginalWarnSize = WarnSize;
	        OriginalMaxDocs = MaxDocs;
	        OriginalWarnDocs = WarnDocs;
            OnEverythingChanged();
        }

	    public override void CheckForChanges()
	    {
		    if (MaxSize != OriginalMaxSize || WarnSize != OriginalWarnSize || MaxDocs != OriginalMaxDocs ||
		        WarnDocs != OriginalWarnDocs)
			    HasUnsavedChanges = true;
	    }

	    public override void MarkAsSaved()
	    {
		    HasUnsavedChanges = false;

			OriginalMaxSize = MaxSize;
			OriginalWarnSize = WarnSize;
			OriginalMaxDocs = MaxDocs;
			OriginalWarnDocs = WarnDocs;
	    }

	    private static long ReadSettingAsInt(DatabaseDocument document, string settingName)
        {
			long value;
            if (document.Settings.ContainsKey(settingName))
				long.TryParse(document.Settings[settingName], out value);
			else
				value = long.MaxValue;

            return value;
        }

	   
    }
}
