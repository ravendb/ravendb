namespace Raven.VisualHost
{
	partial class Shell
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
			this.menuStrip1 = new System.Windows.Forms.MenuStrip();
			this.optionsToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
			this.clearToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
			this.toolStripMenuItem1 = new System.Windows.Forms.ToolStripSeparator();
			this.ignoreHiloToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
			this.setupMasterMasterReplicationToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
			this.setupSlaveMasterReplicationToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
			this.ravenOverfowToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
			this.setupDatabasesToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
			this.stopIndexingToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
			this.sToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
			this.StartServers = new System.Windows.Forms.Button();
			this.groupBox1 = new System.Windows.Forms.GroupBox();
			this.NumberOfServers = new System.Windows.Forms.NumericUpDown();
			this.label1 = new System.Windows.Forms.Label();
			this.ServerTabs = new System.Windows.Forms.TabControl();
			this.menuStrip1.SuspendLayout();
			this.groupBox1.SuspendLayout();
			((System.ComponentModel.ISupportInitialize)(this.NumberOfServers)).BeginInit();
			this.SuspendLayout();
			// 
			// menuStrip1
			// 
			this.menuStrip1.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.optionsToolStripMenuItem,
            this.ravenOverfowToolStripMenuItem,
            this.stopIndexingToolStripMenuItem});
			this.menuStrip1.Location = new System.Drawing.Point(0, 0);
			this.menuStrip1.Name = "menuStrip1";
			this.menuStrip1.Size = new System.Drawing.Size(560, 24);
			this.menuStrip1.TabIndex = 0;
			this.menuStrip1.Text = "menuStrip1";
			// 
			// optionsToolStripMenuItem
			// 
			this.optionsToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.clearToolStripMenuItem,
            this.toolStripMenuItem1,
            this.ignoreHiloToolStripMenuItem,
            this.setupMasterMasterReplicationToolStripMenuItem,
            this.setupSlaveMasterReplicationToolStripMenuItem});
			this.optionsToolStripMenuItem.Name = "optionsToolStripMenuItem";
			this.optionsToolStripMenuItem.Size = new System.Drawing.Size(61, 20);
			this.optionsToolStripMenuItem.Text = "&Options";
			// 
			// clearToolStripMenuItem
			// 
			this.clearToolStripMenuItem.Name = "clearToolStripMenuItem";
			this.clearToolStripMenuItem.Size = new System.Drawing.Size(246, 22);
			this.clearToolStripMenuItem.Text = "&Clear";
			this.clearToolStripMenuItem.Click += new System.EventHandler(this.clearToolStripMenuItem_Click);
			// 
			// toolStripMenuItem1
			// 
			this.toolStripMenuItem1.Name = "toolStripMenuItem1";
			this.toolStripMenuItem1.Size = new System.Drawing.Size(243, 6);
			// 
			// ignoreHiloToolStripMenuItem
			// 
			this.ignoreHiloToolStripMenuItem.Checked = true;
			this.ignoreHiloToolStripMenuItem.CheckState = System.Windows.Forms.CheckState.Checked;
			this.ignoreHiloToolStripMenuItem.Name = "ignoreHiloToolStripMenuItem";
			this.ignoreHiloToolStripMenuItem.Size = new System.Drawing.Size(246, 22);
			this.ignoreHiloToolStripMenuItem.Text = "Ignore &Hilo";
			this.ignoreHiloToolStripMenuItem.Click += new System.EventHandler(this.ignoreHiloToolStripMenuItem_Click);
			// 
			// setupMasterMasterReplicationToolStripMenuItem
			// 
			this.setupMasterMasterReplicationToolStripMenuItem.Name = "setupMasterMasterReplicationToolStripMenuItem";
			this.setupMasterMasterReplicationToolStripMenuItem.Size = new System.Drawing.Size(246, 22);
			this.setupMasterMasterReplicationToolStripMenuItem.Text = "Setup &Master/Master Replication";
			this.setupMasterMasterReplicationToolStripMenuItem.Click += new System.EventHandler(this.setupMasterMasterReplicationToolStripMenuItem_Click);
			// 
			// setupSlaveMasterReplicationToolStripMenuItem
			// 
			this.setupSlaveMasterReplicationToolStripMenuItem.Name = "setupSlaveMasterReplicationToolStripMenuItem";
			this.setupSlaveMasterReplicationToolStripMenuItem.Size = new System.Drawing.Size(246, 22);
			this.setupSlaveMasterReplicationToolStripMenuItem.Text = "Setup &Slave/Master Replication";
			this.setupSlaveMasterReplicationToolStripMenuItem.Click += new System.EventHandler(this.setupSlaveMasterReplicationToolStripMenuItem_Click);
			// 
			// ravenOverfowToolStripMenuItem
			// 
			this.ravenOverfowToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.setupDatabasesToolStripMenuItem});
			this.ravenOverfowToolStripMenuItem.Name = "ravenOverfowToolStripMenuItem";
			this.ravenOverfowToolStripMenuItem.Size = new System.Drawing.Size(99, 20);
			this.ravenOverfowToolStripMenuItem.Text = "&Raven Overfow";
			// 
			// setupDatabasesToolStripMenuItem
			// 
			this.setupDatabasesToolStripMenuItem.Name = "setupDatabasesToolStripMenuItem";
			this.setupDatabasesToolStripMenuItem.Size = new System.Drawing.Size(154, 22);
			this.setupDatabasesToolStripMenuItem.Text = "&Setup Databases";
			this.setupDatabasesToolStripMenuItem.Click += new System.EventHandler(this.setupDatabasesToolStripMenuItem_Click);
			// 
			// stopIndexingToolStripMenuItem
			// 
			this.stopIndexingToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.sToolStripMenuItem});
			this.stopIndexingToolStripMenuItem.Name = "stopIndexingToolStripMenuItem";
			this.stopIndexingToolStripMenuItem.Size = new System.Drawing.Size(64, 20);
			this.stopIndexingToolStripMenuItem.Text = "&Indexing";
			// 
			// sToolStripMenuItem
			// 
			this.sToolStripMenuItem.Name = "sToolStripMenuItem";
			this.sToolStripMenuItem.Size = new System.Drawing.Size(152, 22);
			this.sToolStripMenuItem.Text = "&Stop";
			this.sToolStripMenuItem.Click += new System.EventHandler(this.sToolStripMenuItem_Click);
			// 
			// StartServers
			// 
			this.StartServers.Location = new System.Drawing.Point(190, 25);
			this.StartServers.Name = "StartServers";
			this.StartServers.Size = new System.Drawing.Size(75, 23);
			this.StartServers.TabIndex = 2;
			this.StartServers.Text = "Start";
			this.StartServers.UseVisualStyleBackColor = true;
			this.StartServers.Click += new System.EventHandler(this.StartServers_Click);
			// 
			// groupBox1
			// 
			this.groupBox1.Controls.Add(this.StartServers);
			this.groupBox1.Controls.Add(this.NumberOfServers);
			this.groupBox1.Controls.Add(this.label1);
			this.groupBox1.Dock = System.Windows.Forms.DockStyle.Top;
			this.groupBox1.Location = new System.Drawing.Point(0, 24);
			this.groupBox1.Name = "groupBox1";
			this.groupBox1.Size = new System.Drawing.Size(560, 68);
			this.groupBox1.TabIndex = 2;
			this.groupBox1.TabStop = false;
			this.groupBox1.Text = "Servers";
			// 
			// NumberOfServers
			// 
			this.NumberOfServers.Location = new System.Drawing.Point(116, 25);
			this.NumberOfServers.Name = "NumberOfServers";
			this.NumberOfServers.Size = new System.Drawing.Size(54, 20);
			this.NumberOfServers.TabIndex = 1;
			this.NumberOfServers.Value = new decimal(new int[] {
            1,
            0,
            0,
            0});
			// 
			// label1
			// 
			this.label1.AutoSize = true;
			this.label1.Location = new System.Drawing.Point(12, 27);
			this.label1.Name = "label1";
			this.label1.Size = new System.Drawing.Size(98, 13);
			this.label1.TabIndex = 0;
			this.label1.Text = "Number of Servers:";
			// 
			// ServerTabs
			// 
			this.ServerTabs.Dock = System.Windows.Forms.DockStyle.Fill;
			this.ServerTabs.Location = new System.Drawing.Point(0, 92);
			this.ServerTabs.Name = "ServerTabs";
			this.ServerTabs.SelectedIndex = 0;
			this.ServerTabs.Size = new System.Drawing.Size(560, 306);
			this.ServerTabs.TabIndex = 3;
			// 
			// Shell
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.ClientSize = new System.Drawing.Size(560, 398);
			this.Controls.Add(this.ServerTabs);
			this.Controls.Add(this.groupBox1);
			this.Controls.Add(this.menuStrip1);
			this.Name = "Shell";
			this.Text = "RavenDB Visual Host";
			this.menuStrip1.ResumeLayout(false);
			this.menuStrip1.PerformLayout();
			this.groupBox1.ResumeLayout(false);
			this.groupBox1.PerformLayout();
			((System.ComponentModel.ISupportInitialize)(this.NumberOfServers)).EndInit();
			this.ResumeLayout(false);
			this.PerformLayout();

		}

		#endregion

		private System.Windows.Forms.MenuStrip menuStrip1;
		private System.Windows.Forms.ToolStripMenuItem optionsToolStripMenuItem;
		private System.Windows.Forms.ToolStripMenuItem clearToolStripMenuItem;
		private System.Windows.Forms.Button StartServers;
		private System.Windows.Forms.GroupBox groupBox1;
		private System.Windows.Forms.NumericUpDown NumberOfServers;
		private System.Windows.Forms.Label label1;
		private System.Windows.Forms.TabControl ServerTabs;
		private System.Windows.Forms.ToolStripMenuItem ignoreHiloToolStripMenuItem;
		private System.Windows.Forms.ToolStripMenuItem setupMasterMasterReplicationToolStripMenuItem;
		private System.Windows.Forms.ToolStripMenuItem setupSlaveMasterReplicationToolStripMenuItem;
		private System.Windows.Forms.ToolStripSeparator toolStripMenuItem1;
		private System.Windows.Forms.ToolStripMenuItem ravenOverfowToolStripMenuItem;
		private System.Windows.Forms.ToolStripMenuItem setupDatabasesToolStripMenuItem;
		private System.Windows.Forms.ToolStripMenuItem stopIndexingToolStripMenuItem;
		private System.Windows.Forms.ToolStripMenuItem sToolStripMenuItem;

	}
}

