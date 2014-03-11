using System;
using System.Collections.Generic;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Tagging;
using ActiproSoftware.Windows.Controls.SyntaxEditor;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt.Implementation;
using System.Linq;

namespace Raven.Studio.Features.JsonEditor
{
    public class LinkTagQuickInfoProvider : QuickInfoProviderBase
    {
        public override object GetContext(IHitTestResult hitTestResult)
        {
            if (hitTestResult.Type == HitTestResultType.ViewTextAreaOverCharacter)
                return GetContext(hitTestResult.View, hitTestResult.Offset);

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
                return false;

            var linkTag = linkTagContext.TagRange.Tag;
            var session = new QuickInfoSession()
            {
                Context = context,
            };

            if (linkTag.NavigationType == LinkTagNavigationType.ExternalUrl)
            {
				var htmlSnippet = string.Format("{0}{1}Shift + Click to follow link", linkTag.Url, Environment.NewLine);
                session.Content = new PlainTextContentProvider(htmlSnippet).GetContent();
            }
            else
            {
                session.Content = new QuickDocumentView() {DocumentId = linkTag.Url};
            }

            session.Open(view, linkTagContext.TagRange.SnapshotRange);

            return true;
        }

        protected override IEnumerable<Type> ContextTypes
        {
            get { return new[] {typeof (LinkTagContext)}; }
        }
    }

    internal class LinkTagContext
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
}