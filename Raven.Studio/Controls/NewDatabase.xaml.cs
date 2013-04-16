using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Database.Plugins;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;
using Raven.Studio.Infrastructure.Converters;
using Raven.Studio.Models;
using Raven.Client.Connection;

namespace Raven.Studio.Controls
{
	public partial class NewDatabase : ChildWindow
	{
		public List<string> Bundles { get; private set; }
		public Observable<LicensingStatus> LicensingStatus { get; set; }
		public Observable<bool> HasAuth { get; set; }

		public NewDatabase()
		{
			HasAuth = new Observable<bool>();
			LicensingStatus = new Observable<LicensingStatus>();
			InitializeComponent();
			var req = ApplicationModel.DatabaseCommands.ForSystemDatabase().CreateRequest("/license/status", "GET");

			req.ReadResponseJsonAsync().ContinueOnSuccessInTheUIThread(doc =>
			{
				LicensingStatus.Value = ((RavenJObject)doc).Deserialize<LicensingStatus>(new DocumentConvention());
				var hasPeriodic =(bool) new BundleNameToActiveConverter().Convert(LicensingStatus.Value, typeof (bool), "PeriodicBackup",
				                                                            CultureInfo.InvariantCulture);
				if (hasPeriodic)
				{
					Bundles.Add("PeriodicBackup");
				}
			});

			req = ApplicationModel.DatabaseCommands.ForSystemDatabase().CreateRequest("/plugins/status".NoCache(), "GET");

			req.ReadResponseJsonAsync().ContinueOnSuccessInTheUIThread(item =>
			{
				var plugins = ((RavenJObject)item).Deserialize<PluginsStatus>(new DocumentConvention());

				if (plugins == null || plugins.Plugins.Contains("Raven.Bundles.Authorization") == false)
					return;

				HasAuth.Value = true;
			});

			
			Bundles = new List<string>();
			KeyDown += (sender, args) =>
			{
				if (args.Key == Key.Escape)
					DialogResult = false;
			};
		}

		private void OKButton_Click(object sender, RoutedEventArgs e)
		{
			DialogResult = true;
		}

		private void CancelButton_Click(object sender, RoutedEventArgs e)
		{
			DialogResult = false;
		}

		private void CheckBox_Checked(object sender, RoutedEventArgs e)
		{
			AdvancedSettings.Visibility = Visibility.Visible;
		}

		private void CheckBox_Unchecked(object sender, RoutedEventArgs e)
		{
			AdvancedSettings.Visibility = Visibility.Collapsed;
		}

		private void DbName_KeyDown(object sender, System.Windows.Input.KeyEventArgs e)
		{
			if (e.Key == Key.Enter && !string.IsNullOrWhiteSpace(DbName.Text))
				DialogResult = true;
				
		}

		private void Checked(object sender, RoutedEventArgs e)
		{
			var checkbox = sender as CheckBox;
			if (checkbox == null)
				return;
			Bundles.Add(checkbox.Name);

			var border = FindName(checkbox.Name + "Settings") as Border;
			if (border != null)
			{
				border.Visibility = Visibility.Visible;
			}
		}

		private void UnChecked(object sender, RoutedEventArgs e)
		{
			var checkbox = sender as CheckBox;
			if (checkbox == null)
				return;
			Bundles.Remove(checkbox.Name);

			var border = FindName(checkbox.Name + "Settings") as Border;
			if (border != null)
			{
				border.Visibility = Visibility.Collapsed;
			}
		}
	}
}