using System;
using System.Collections.Generic;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;

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
			if (currentSettings.ContainsKey(key))
			{
				return (string)currentSettings[key];
			}
			else
			{
				return "";
			}
		}

		private int GetSettingAsInt(string key)
		{
			if (currentSettings.ContainsKey(key))
			{
				return Convert.ToInt32(currentSettings[key]);
			}
			else
			{
				return 0;
			}
		}
	}
}
