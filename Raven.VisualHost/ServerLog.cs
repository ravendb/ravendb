using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Web;
using System.Windows.Forms;
using Raven.Abstractions.Json;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Server;

namespace Raven.VisualHost
{
	public partial class ServerLog : UserControl
	{
		public int NumOfRequests;

		public ServerLog()
		{
			InitializeComponent();
		}

		public RavenDbServer Server { get; set; }

		public string Url { get; set; }

		public void AddRequest(TrackedRequest request)
		{
			IAsyncResult a = null;
			a = BeginInvoke((Action)(() =>
			{
				var asyncResult = a;
				if (asyncResult != null)
				{
					EndInvoke(asyncResult);
				}
				var url = request.Url;
				var startsIndex = url.IndexOf("&start=");
				if (startsIndex != -1)
					url = url.Substring(0, startsIndex);
				RequestsLists.Items.Add(new ListViewItem(new[]
				{
					request.Method, 
					request.Status.ToString(), 
					HttpUtility.UrlDecode(HttpUtility.UrlDecode(url))
				})
				{
					Tag = request
				});
			}));
		}

		private void RequestsLists_SelectedIndexChanged(object sender, EventArgs e)
		{
			Reset();
			if (RequestsLists.SelectedItems.Count == 0)
			{
				return;
			}
			var trackedRequest = ((TrackedRequest)RequestsLists.SelectedItems[0].Tag);

			if(trackedRequest.Method == "GET" || trackedRequest.Method == "DELETE")
			{
				this.tabControl1.SelectedTab = ResponseTextTab;
			}
			else
			{
				this.tabControl1.SelectedTab = RequestTextTab;
			}

			RequestText.Text = GetRequestText(trackedRequest);
			ResponseText.Text = GetResponseText(trackedRequest);
		}

		private string GetRequestText(TrackedRequest trackedRequest)
		{
			var requestStringBuilder = new StringBuilder();
			//AppendHeaders(trackedRequest.RequestHeaders, requestStringBuilder);

			requestStringBuilder.AppendLine();
			WriteStreamContentMaybeJson(requestStringBuilder, trackedRequest.RequestContent, trackedRequest.RequestHeaders);

			return requestStringBuilder.ToString();
		}

		private string GetResponseText(TrackedRequest trackedRequest)
		{
			var requestStringBuilder = new StringBuilder();
			//AppendHeaders(trackedRequest.ResponseHeaders, requestStringBuilder);

			requestStringBuilder.AppendLine();

			WriteStreamContentMaybeJson(requestStringBuilder, trackedRequest.ResponseContent, trackedRequest.ResponseHeaders);

			return requestStringBuilder.ToString();
		}

		private void WriteStreamContentMaybeJson(StringBuilder requestStringBuilder, Stream stream, NameValueCollection headers)
		{
			stream.Position = 0;

			if(headers["Content-Type"] == "application/json; charset=utf-8" && stream.Length > 0)
			{
				var t = RavenJToken.Load(new RavenJsonTextReader(new StreamReader(stream)));
				requestStringBuilder.Append(t.ToString(Formatting.Indented));
			}
			else
			{
				var streamReader = new StreamReader(stream);
				requestStringBuilder.Append(streamReader.ReadToEnd());	
			}
		}

		private void AppendHeaders(NameValueCollection nameValueCollection, StringBuilder sb)
		{
			foreach (string requestHeader in nameValueCollection)
			{
				var values = nameValueCollection.GetValues(requestHeader);
				if (values == null)
					continue;
				foreach (var value in values)
				{
					sb.Append(requestHeader).Append(": ").Append(value).AppendLine();
				}
			}
		}

		private void Reset()
		{
			ResponseText.Text = string.Empty;
			RequestText.Text = string.Empty;
		}

		public void Clear()
		{
			Reset();
			RequestsLists.Items.Clear();
			NumOfRequests = 0;
		}

		private void KillServer_Click(object sender, EventArgs e)
		{
			Server.Dispose();
			foreach (Control control in Controls)
			{
				control.BackColor =Color.DarkRed;
			}
		}

		public int IncrementRequest()
		{
			return Interlocked.Increment(ref NumOfRequests);
		}
	}
}
