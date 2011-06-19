namespace Raven.Client.Debug
{
	partial class DocumentSessionView
	{
		/// <summary>
		/// Required designer variable.
		/// </summary>
		private System.ComponentModel.IContainer components = null;

		/// <summary>
		/// Clean up any resources being used.
		/// </summary>
		/// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
		protected override void Dispose(bool disposing)
		{
			if (disposing && (components != null))
			{
				components.Dispose();
			}
			base.Dispose(disposing);
		}

		#region Windows Form Designer generated code

		/// <summary>
		/// Required method for Designer support - do not modify
		/// the contents of this method with the code editor.
		/// </summary>
		private void InitializeComponent()
		{
			this.Requests = new System.Windows.Forms.ListView();
			this.Method = ((System.Windows.Forms.ColumnHeader)(new System.Windows.Forms.ColumnHeader()));
			this.HttpResult = ((System.Windows.Forms.ColumnHeader)(new System.Windows.Forms.ColumnHeader()));
			this.Status = ((System.Windows.Forms.ColumnHeader)(new System.Windows.Forms.ColumnHeader()));
			this.Url = ((System.Windows.Forms.ColumnHeader)(new System.Windows.Forms.ColumnHeader()));
			this.splitter1 = new System.Windows.Forms.Splitter();
			this.DetailsTabs = new System.Windows.Forms.TabControl();
			this.Response = new System.Windows.Forms.TabPage();
			this.ResponseText = new System.Windows.Forms.TextBox();
			this.Request = new System.Windows.Forms.TabPage();
			this.RequestText = new System.Windows.Forms.TextBox();
			this.DetailsTabs.SuspendLayout();
			this.Response.SuspendLayout();
			this.Request.SuspendLayout();
			this.SuspendLayout();
			// 
			// Requests
			// 
			this.Requests.Columns.AddRange(new System.Windows.Forms.ColumnHeader[] {
            this.Method,
            this.HttpResult,
            this.Status,
            this.Url});
			this.Requests.Dock = System.Windows.Forms.DockStyle.Top;
			this.Requests.FullRowSelect = true;
			this.Requests.Location = new System.Drawing.Point(0, 0);
			this.Requests.MultiSelect = false;
			this.Requests.Name = "Requests";
			this.Requests.Size = new System.Drawing.Size(650, 141);
			this.Requests.TabIndex = 0;
			this.Requests.UseCompatibleStateImageBehavior = false;
			this.Requests.View = System.Windows.Forms.View.Details;
			this.Requests.SelectedIndexChanged += new System.EventHandler(this.Requests_SelectedIndexChanged);
			// 
			// Method
			// 
			this.Method.Text = "Method";
			this.Method.Width = 80;
			// 
			// HttpResult
			// 
			this.HttpResult.Text = "Http Result";
			this.HttpResult.Width = 85;
			// 
			// Status
			// 
			this.Status.Text = "Status";
			this.Status.Width = 90;
			// 
			// Url
			// 
			this.Url.Text = "Url";
			this.Url.Width = 350;
			// 
			// splitter1
			// 
			this.splitter1.Dock = System.Windows.Forms.DockStyle.Top;
			this.splitter1.Location = new System.Drawing.Point(0, 141);
			this.splitter1.Name = "splitter1";
			this.splitter1.Size = new System.Drawing.Size(650, 3);
			this.splitter1.TabIndex = 1;
			this.splitter1.TabStop = false;
			// 
			// DetailsTabs
			// 
			this.DetailsTabs.Alignment = System.Windows.Forms.TabAlignment.Bottom;
			this.DetailsTabs.Controls.Add(this.Response);
			this.DetailsTabs.Controls.Add(this.Request);
			this.DetailsTabs.Dock = System.Windows.Forms.DockStyle.Fill;
			this.DetailsTabs.Location = new System.Drawing.Point(0, 144);
			this.DetailsTabs.Name = "DetailsTabs";
			this.DetailsTabs.SelectedIndex = 0;
			this.DetailsTabs.Size = new System.Drawing.Size(650, 213);
			this.DetailsTabs.TabIndex = 2;
			// 
			// Response
			// 
			this.Response.Controls.Add(this.ResponseText);
			this.Response.Location = new System.Drawing.Point(4, 4);
			this.Response.Name = "Response";
			this.Response.Padding = new System.Windows.Forms.Padding(3);
			this.Response.Size = new System.Drawing.Size(642, 187);
			this.Response.TabIndex = 0;
			this.Response.Text = "Response";
			this.Response.UseVisualStyleBackColor = true;
			// 
			// ResponseText
			// 
			this.ResponseText.Dock = System.Windows.Forms.DockStyle.Fill;
			this.ResponseText.Location = new System.Drawing.Point(3, 3);
			this.ResponseText.Multiline = true;
			this.ResponseText.Name = "ResponseText";
			this.ResponseText.ReadOnly = true;
			this.ResponseText.Size = new System.Drawing.Size(636, 181);
			this.ResponseText.TabIndex = 0;
			// 
			// Request
			// 
			this.Request.Controls.Add(this.RequestText);
			this.Request.Location = new System.Drawing.Point(4, 4);
			this.Request.Name = "Request";
			this.Request.Padding = new System.Windows.Forms.Padding(3);
			this.Request.Size = new System.Drawing.Size(642, 187);
			this.Request.TabIndex = 1;
			this.Request.Text = "Request";
			this.Request.UseVisualStyleBackColor = true;
			// 
			// RequestText
			// 
			this.RequestText.Dock = System.Windows.Forms.DockStyle.Fill;
			this.RequestText.Location = new System.Drawing.Point(3, 3);
			this.RequestText.Multiline = true;
			this.RequestText.Name = "RequestText";
			this.RequestText.ReadOnly = true;
			this.RequestText.Size = new System.Drawing.Size(636, 181);
			this.RequestText.TabIndex = 0;
			// 
			// DocumentSessionView
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.ClientSize = new System.Drawing.Size(650, 357);
			this.Controls.Add(this.DetailsTabs);
			this.Controls.Add(this.splitter1);
			this.Controls.Add(this.Requests);
			this.Name = "DocumentSessionView";
			this.Text = "DocumentSessionView";
			this.Load += new System.EventHandler(this.DocumentSessionView_Load);
			this.DetailsTabs.ResumeLayout(false);
			this.Response.ResumeLayout(false);
			this.Response.PerformLayout();
			this.Request.ResumeLayout(false);
			this.Request.PerformLayout();
			this.ResumeLayout(false);

		}

		#endregion

		private System.Windows.Forms.ListView Requests;
		private System.Windows.Forms.ColumnHeader Method;
		private System.Windows.Forms.ColumnHeader HttpResult;
		private System.Windows.Forms.ColumnHeader Url;
		private System.Windows.Forms.Splitter splitter1;
		private System.Windows.Forms.TabControl DetailsTabs;
		private System.Windows.Forms.TabPage Response;
		private System.Windows.Forms.TextBox ResponseText;
		private System.Windows.Forms.TabPage Request;
		private System.Windows.Forms.TextBox RequestText;
		private System.Windows.Forms.ColumnHeader Status;
	}
}