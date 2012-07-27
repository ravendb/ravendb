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
using ActiproSoftware.Text;
using ActiproSoftware.Text.Tagging;
using ActiproSoftware.Windows.Controls.SyntaxEditor;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt.Implementation;
using System.Linq;

namespace Raven.Studio.Features.JsonEditor
{
    public class LinkTagQuickInfoProvider : QuickInfoProviderBase
    {
        private class LinkTagContext
        {
            public TagSnapshotRange<LinkTag> TagRange { get; set; }

            public bool Equals(LinkTagContext other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return Equals(other.TagRange, TagRange);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != typeof (LinkTagContext)) return false;
                return Equals((LinkTagContext) obj);
            }

            public override int GetHashCode()
            {
                return (TagRange != null ? TagRange.GetHashCode() : 0);
            }
        }

        public override object GetContext(IHitTestResult hitTestResult)
        {
            if (hitTestResult.Type == HitTestResultType.ViewTextAreaOverCharacter)
            {
                return GetContext(hitTestResult.View, hitTestResult.Offset);
            }

            return null;
        }

        public override object GetContext(IEditorView view, int offset)
        {
            using (var tagAggregator = view.CreateTagAggregator<LinkTag>())
            {
                var tagRange = tagAggregator.GetTags(new[] {new TextSnapshotRange(view.CurrentSnapshot, offset)})
                    .FirstOrDefault(tag => tag.Tag != null && tag.SnapshotRange.Contains(offset));

                return tagRange != null ? new LinkTagContext {TagRange = tagRange} : null;
            }
        }

        protected override bool RequestSession(IEditorView view, object context)
        {
            var linkTagContext = context as LinkTagContext;
            if (linkTagContext == null)
            {
                return false;
            }

            var linkTag = linkTagContext.TagRange.Tag;
            var htmlSnippet = linkTag.NavigationType == LinkTagNavigationType.ExternalUrl ? "Shift-Click to navigate to Url " + linkTag.Url
                : "Shift-Click to navigate to document " + linkTag.Url;

            var session = new QuickInfoSession()
            {
                Context = context,
                Content = new HtmlContentProvider(htmlSnippet).GetContent()
            };

            session.Open(view, linkTagContext.TagRange.SnapshotRange);

            return true;
        }

        protected override IEnumerable<Type> ContextTypes
        {
            get { return new[] {typeof (LinkTagContext)}; }
        }
    }
}
