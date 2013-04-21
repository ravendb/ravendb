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

        public int MaxSize { get; set; }
        public int WarnSize { get; set; }
        public int MaxDocs { get; set; }
        public int WarnDocs { get; set; }

        public override void LoadFor(DatabaseDocument document)
        {
            MaxSize = ReadSettingAsInt(document, Constants.SizeHardLimitInKB)/1024;
            WarnSize = ReadSettingAsInt(document, Constants.SizeSoftLimitInKB)/1024;
            MaxDocs = ReadSettingAsInt(document, Constants.DocsHardLimit);
            WarnDocs = ReadSettingAsInt(document, Constants.DocsSoftLimit);

            OnEverythingChanged();
        }

        private static int ReadSettingAsInt(DatabaseDocument document, string settingName)
        {
            var value = 0;
            if (document.Settings.ContainsKey(settingName))
                int.TryParse(document.Settings[settingName], out value);
            return value;
        }
    }
}