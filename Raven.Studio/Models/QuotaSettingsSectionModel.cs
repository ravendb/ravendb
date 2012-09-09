using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Abstractions.Data;

namespace Raven.Studio.Models
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
            int value = 0;
            if (document.Settings.ContainsKey(settingName))
                int.TryParse(document.Settings[settingName], out value);
            return value;
        }
    }
}
