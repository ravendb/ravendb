using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Client.Connection.Profiling;

namespace Raven.Client.Debug
{
	public partial class DocumentSessionView : Form
	{
		public DocumentSessionView()
		{
			InitializeComponent();
		}

		public ProfilingInformation ProfilingInformation { get; set; }

		private void DocumentSessionView_Load(object sender, EventArgs e)
		{
			foreach (var request in ProfilingInformation.Requests)
			{
				Requests.Items.Add(new ListViewItem(
				                   	new[]
				                   	{
				                   		request.Method, request.HttpResult.ToString(), request.Status.ToString(),
				                   		request.Url.ToString()
				                   	},
				                   	-1
				                   	)
				{
					Tag = request
				});
			}
		}

		private void Requests_SelectedIndexChanged(object sender, EventArgs e)
		{
			if (Requests.SelectedItems.Count != 1)
				return;
			var request = (RequestResultArgs)Requests.SelectedItems[0].Tag;
			ResponseText.Text = TryFormatJson(request.Result);
			RequestText.Text = TryFormatJson(request.PostedData);
		}

		private string TryFormatJson(string data)
		{
			try
			{
				return JToken.Parse(data).ToString(Formatting.Indented);
			}
			catch (Exception)
			{
				return data;
			}
		}
	}
}
