using Raven.Abstractions.Data;

namespace Raven.Studio.Models
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

        public override void LoadFor(DatabaseDocument document)
        {
			MaxSize = ReadSettingAsInt(document, Constants.SizeHardLimitInKB) / 1024;
			WarnSize = ReadSettingAsInt(document, Constants.SizeSoftLimitInKB) / 1024;
            MaxDocs = ReadSettingAsInt(document, Constants.DocsHardLimit);
            WarnDocs = ReadSettingAsInt(document, Constants.DocsSoftLimit);

            OnEverythingChanged();
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
