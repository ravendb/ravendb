namespace Raven.VisualHost
{
	partial class ServerLog
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
			this.RequestsLists = new System.Windows.Forms.ListView();
			this.columnHeader1 = ((System.Windows.Forms.ColumnHeader)(new System.Windows.Forms.ColumnHeader()));
			this.columnHeader3 = ((System.Windows.Forms.ColumnHeader)(new System.Windows.Forms.ColumnHeader()));
			this.columnHeader2 = ((System.Windows.Forms.ColumnHeader)(new System.Windows.Forms.ColumnHeader()));
			this.splitter1 = new System.Windows.Forms.Splitter();
			this.tabControl1 = new System.Windows.Forms.TabControl();
			this.ResponseTextTab = new System.Windows.Forms.TabPage();
			this.ResponseText = new System.Windows.Forms.TextBox();
			this.RequestTextTab = new System.Windows.Forms.TabPage();
			this.RequestText = new System.Windows.Forms.TextBox();
			this.panel1 = new System.Windows.Forms.Panel();
			this.KillServer = new System.Windows.Forms.Button();
			this.tabControl1.SuspendLayout();
			this.ResponseTextTab.SuspendLayout();
			this.RequestTextTab.SuspendLayout();
			this.panel1.SuspendLayout();
			this.SuspendLayout();
			// 
			// RequestsLists
			// 
			this.RequestsLists.Columns.AddRange(new System.Windows.Forms.ColumnHeader[] {
            this.columnHeader1,
            this.columnHeader3,
            this.columnHeader2});
			this.RequestsLists.Dock = System.Windows.Forms.DockStyle.Top;
			this.RequestsLists.FullRowSelect = true;
			this.RequestsLists.HideSelection = false;
			this.RequestsLists.Location = new System.Drawing.Point(0, 0);
			this.RequestsLists.MultiSelect = false;
			this.RequestsLists.Name = "RequestsLists";
			this.RequestsLists.Size = new System.Drawing.Size(552, 123);
			this.RequestsLists.TabIndex = 1;
			this.RequestsLists.UseCompatibleStateImageBehavior = false;
			this.RequestsLists.View = System.Windows.Forms.View.Details;
			this.RequestsLists.SelectedIndexChanged += new System.EventHandler(this.RequestsLists_SelectedIndexChanged);
			// 
			// columnHeader1
			// 
			this.columnHeader1.Text = "Method";
			// 
			// columnHeader3
			// 
			this.columnHeader3.Text = "Http Status";
			this.columnHeader3.Width = 77;
			// 
			// columnHeader2
			// 
			this.columnHeader2.Text = "Url";
			this.columnHeader2.Width = 400;
			// 
			// splitter1
			// 
			this.splitter1.Dock = System.Windows.Forms.DockStyle.Top;
			this.splitter1.Location = new System.Drawing.Point(0, 123);
			this.splitter1.Name = "splitter1";
			this.splitter1.Size = new System.Drawing.Size(552, 3);
			this.splitter1.TabIndex = 2;
			this.splitter1.TabStop = false;
			// 
			// tabControl1
			// 
			this.tabControl1.Alignment = System.Windows.Forms.TabAlignment.Bottom;
			this.tabControl1.Controls.Add(this.ResponseTextTab);
			this.tabControl1.Controls.Add(this.RequestTextTab);
			this.tabControl1.Dock = System.Windows.Forms.DockStyle.Fill;
			this.tabControl1.Location = new System.Drawing.Point(0, 126);
			this.tabControl1.Name = "tabControl1";
			this.tabControl1.SelectedIndex = 0;
			this.tabControl1.Size = new System.Drawing.Size(552, 147);
			this.tabControl1.TabIndex = 3;
			// 
			// ResponseTextTab
			// 
			this.ResponseTextTab.Controls.Add(this.ResponseText);
			this.ResponseTextTab.Location = new System.Drawing.Point(4, 4);
			this.ResponseTextTab.Name = "ResponseTextTab";
			this.ResponseTextTab.Padding = new System.Windows.Forms.Padding(3);
			this.ResponseTextTab.Size = new System.Drawing.Size(544, 121);
			this.ResponseTextTab.TabIndex = 1;
			this.ResponseTextTab.Text = "ResponseDetails";
			this.ResponseTextTab.UseVisualStyleBackColor = true;
			// 
			// ResponseText
			// 
			this.ResponseText.Dock = System.Windows.Forms.DockStyle.Fill;
			this.ResponseText.Location = new System.Drawing.Point(3, 3);
			this.ResponseText.Multiline = true;
			this.ResponseText.Name = "ResponseText";
			this.ResponseText.ReadOnly = true;
			this.ResponseText.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
			this.ResponseText.Size = new System.Drawing.Size(538, 115);
			this.ResponseText.TabIndex = 1;
			this.ResponseText.WordWrap = false;
			// 
			// RequestTextTab
			// 
			this.RequestTextTab.Controls.Add(this.RequestText);
			this.RequestTextTab.Location = new System.Drawing.Point(4, 4);
			this.RequestTextTab.Name = "RequestTextTab";
			this.RequestTextTab.Padding = new System.Windows.Forms.Padding(3);
			this.RequestTextTab.Size = new System.Drawing.Size(544, 149);
			this.RequestTextTab.TabIndex = 0;
			this.RequestTextTab.Text = "RequestDetails";
			this.RequestTextTab.UseVisualStyleBackColor = true;
			// 
			// RequestText
			// 
			this.RequestText.Dock = System.Windows.Forms.DockStyle.Fill;
			this.RequestText.Location = new System.Drawing.Point(3, 3);
			this.RequestText.Multiline = true;
			this.RequestText.Name = "RequestText";
			this.RequestText.ReadOnly = true;
			this.RequestText.ScrollBars = System.Windows.Forms.ScrollBars.Vertical;
			this.RequestText.Size = new System.Drawing.Size(538, 143);
			this.RequestText.TabIndex = 0;
			this.RequestText.WordWrap = false;
			// 
			// panel1
			// 
			this.panel1.Controls.Add(this.KillServer);
			this.panel1.Dock = System.Windows.Forms.DockStyle.Bottom;
			this.panel1.Location = new System.Drawing.Point(0, 273);
			this.panel1.Name = "panel1";
			this.panel1.Size = new System.Drawing.Size(552, 28);
			this.panel1.TabIndex = 4;
			// 
			// KillServer
			// 
			this.KillServer.Dock = System.Windows.Forms.DockStyle.Right;
			this.KillServer.Location = new System.Drawing.Point(477, 0);
			this.KillServer.Name = "KillServer";
			this.KillServer.Size = new System.Drawing.Size(75, 28);
			this.KillServer.TabIndex = 0;
			this.KillServer.Text = "Kill Server";
			this.KillServer.UseVisualStyleBackColor = true;
			this.KillServer.Click += new System.EventHandler(this.KillServer_Click);
			// 
			// ServerLog
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.Controls.Add(this.tabControl1);
			this.Controls.Add(this.panel1);
			this.Controls.Add(this.splitter1);
			this.Controls.Add(this.RequestsLists);
			this.Name = "ServerLog";
			this.Size = new System.Drawing.Size(552, 301);
			this.tabControl1.ResumeLayout(false);
			this.ResponseTextTab.ResumeLayout(false);
			this.ResponseTextTab.PerformLayout();
			this.RequestTextTab.ResumeLayout(false);
			this.RequestTextTab.PerformLayout();
			this.panel1.ResumeLayout(false);
			this.ResumeLayout(false);

		}

		#endregion

		private System.Windows.Forms.ListView RequestsLists;
		private System.Windows.Forms.ColumnHeader columnHeader1;
		private System.Windows.Forms.ColumnHeader columnHeader2;
		private System.Windows.Forms.Splitter splitter1;
		private System.Windows.Forms.TabControl tabControl1;
		private System.Windows.Forms.TabPage RequestTextTab;
		private System.Windows.Forms.TabPage ResponseTextTab;
		private System.Windows.Forms.TextBox RequestText;
		private System.Windows.Forms.TextBox ResponseText;
		private System.Windows.Forms.ColumnHeader columnHeader3;
		private System.Windows.Forms.Panel panel1;
		private System.Windows.Forms.Button KillServer;
	}
}