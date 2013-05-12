using System;
using System.Collections.Generic;

namespace Raven.Studio.Infrastructure
{
	public class Settings
	{
		public static readonly Settings Instance = new Settings(); 
		Dictionary<string, object> currentSettings = new Dictionary<string, object>();
 
		public void LoadSettings(IDictionary<string,object> settings)
		{
			foreach (var setting in settings)
			{
				currentSettings[setting.Key] = setting.Value;
			}    
		}

		public void SaveSettings(IDictionary<string, object> settings)
		{
			foreach (var setting in currentSettings)
			{
				settings[setting.Key] = setting.Value;
			}
		}

	    public bool AutoRefreshDocumentLists
	    {
	        get { return GetSettingAsBool("AutoRefreshDocumentLists", true); }
            set { currentSettings["AutoRefreshDocumentLists"] = value; }
	    }

		public int DocumentSize
		{
			get { return GetSettingAsInt("DocumentSize"); }
			set { currentSettings["DocumentSize"] = value; }
		}

		public string LastUrl
		{
			get { return GetSettingAsString("LastUrl"); }
			set { currentSettings["LastUrl"] = value; }
		}

		public string SelectedDatabase
		{
			get { return GetSettingAsString("SelectedDatabase"); }
			set { currentSettings["SelectedDatabase"] = value; }
		}

		public string DocumentOutliningMode
		{
			get { return GetSettingAsString("DocumentOutliningMode"); }
			set { currentSettings["DocumentOutliningMode"] = value; }
		}

		private string GetSettingAsString(string key)
		{
		    return currentSettings.ContainsKey(key) ? (string) currentSettings[key] : "";
		}

	    private int GetSettingAsInt(string key)
	    {
	        return currentSettings.ContainsKey(key) ? Convert.ToInt32(currentSettings[key]) : 0;
	    }

        private bool GetSettingAsBool(string key, bool defaultValue = false)
        {
            return currentSettings.ContainsKey(key) ? Convert.ToBoolean(currentSettings[key]) : defaultValue;
        }
	}
}