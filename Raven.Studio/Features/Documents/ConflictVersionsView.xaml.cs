using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Json.Linq;
using Raven.Studio.Commands;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Documents
{
    public partial class ConflictVersionsView : UserControl
    {
        public ConflictVersionsView()
        {
            InitializeComponent();
        }

        public RavenJArray ConflictVersionInfo
        {
            get { return (RavenJArray)GetValue(ConflictVersionInfoProperty); }
            set { SetValue(ConflictVersionInfoProperty, value); }
        }

        public IDictionary<string,ReplicationSourceInfo> ReplicationSourcesLookup
        {
            get { return (IDictionary<string, ReplicationSourceInfo>)GetValue(ReplicationSourcesLookupProperty); }
            set { SetValue(ReplicationSourcesLookupProperty, value); }
        }

        public static readonly DependencyProperty ReplicationSourcesLookupProperty =
            DependencyProperty.Register("ReplicationSourcesLookup", typeof(IDictionary<string, ReplicationSourceInfo>), typeof(ConflictVersionsView), new PropertyMetadata(null, OnVersionsChanged));

        public static readonly DependencyProperty ConflictVersionInfoProperty =
            DependencyProperty.Register("ConflictVersionInfo", typeof(RavenJArray), typeof(ConflictVersionsView), new PropertyMetadata(null, OnVersionsChanged));

        private static void OnVersionsChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            (d as ConflictVersionsView).UpdateVersions();
        }

        private void UpdateVersions()
        {
            TextBlock.Blocks.Clear();
            
            if (ConflictVersionInfo == null)
            {
                return;
            }

            var para = new Paragraph();
            var navigateCommand = new NavigateToCommand();
            var count = 1;

            foreach (var version in ConflictVersionInfo)
            {
                var versionId = version.Value<string>("Id");
                var sourceId = version.Value<string>("SourceId");

                ReplicationSourceInfo sourceInfo = null;

                if (ReplicationSourcesLookup != null)
                {
                    ReplicationSourcesLookup.TryGetValue(sourceId, out sourceInfo);
                }

                var hyperlink = new InlineUIContainer()
                {
                    Child = new HyperlinkButton()
                    {
                        Content = count.ToString() + (sourceInfo != null ? " (" + sourceInfo.Name + ")" : ""),
                        Command = navigateCommand,
                        CommandParameter = "/edit?id=" + versionId
                    }
                };

                if (sourceInfo != null)
                {
                    var toolTip = CreateToolTip(versionId, sourceInfo);
                    ToolTipService.SetToolTip(hyperlink.Child, toolTip);
                }

                para.Inlines.Add(hyperlink);
                para.Inlines.Add(" ");

                count++;
            }

            TextBlock.Blocks.Add(para);
        }

        private object CreateToolTip(string versionId, ReplicationSourceInfo sourceInfo)
        {
            var textBlock = new TextBlock()
            {
                Inlines =
                {
                    new Bold() {Inlines = {new Run {Text = "Document Id:"}}},
                    new Run() {Text = " "},
                    new Run {Text = versionId},
                    new LineBreak(),
                    new Bold() {Inlines = {new Run {Text = "Server Url:"}}},
                    new Run() {Text = " "},
                    new Run {Text = sourceInfo.Url},
                }
            };

            return textBlock;
        }
    }
}
