using System;
using System.Collections.Generic;
using System.Net;
using System.Text.RegularExpressions;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Parsing.LLParser.Implementation;
using ActiproSoftware.Text.Tagging;
using ActiproSoftware.Text.Tagging.Implementation;
using ActiproSoftware.Text.Utility;

namespace Raven.Studio.Features.JsonEditor
{
    public class PropertyValueLinkTagger : CollectionTagger<LinkTag>
    {
        private readonly DocumentReferencedIdManager idManager;

        public PropertyValueLinkTagger(ICodeDocument document) : base("PropertyValueLinkTagger", null, document, true)
        {
            document.ParseDataChanged += delegate { UpdateTags(); };
            idManager = document.Properties.GetOrCreateSingleton(() => new DocumentReferencedIdManager());
            idManager.Changed += delegate { UpdateTags(); };
        }

        private void UpdateTags()
        {
            using (CreateBatch())
            {
                Clear();

                var jsonStringNodes = Document.FindAllStringValueNodes();
                var snapshot = GetSnapshot(Document);

                foreach (var stringNode in jsonStringNodes)
                {
                    var linkTag = GetLinkTag(stringNode);

                    if (linkTag == null)
                    {
                        continue;
                    }

                    if (stringNode.StartOffset.HasValue && stringNode.EndOffset.HasValue)
                    {
                        var range = new TextSnapshotRange(snapshot, stringNode.StartOffset.Value + 1,
                                                          stringNode.EndOffset.Value - 1);

                        Add(new TagVersionRange<LinkTag>(range, TextRangeTrackingModes.Default, linkTag));
                    }
                }
            }
        }

        private LinkTag GetLinkTag(JsonStringNode stringNode)
        {
            if (string.IsNullOrEmpty(stringNode.Text))
            {
                return null;
            }

            if (idManager.IsId(stringNode.Text))
            {
                return new LinkTag()
                {
                    NavigationType = LinkTagNavigationType.Document,
                    Url = stringNode.Text
                };
            }
            else if (IsUrl(stringNode.Text))
            {
                return new LinkTag() { NavigationType = LinkTagNavigationType.ExternalUrl, Url = stringNode.Text };
            }
            else
            {
                return null;
            }
        }

        private bool IsUrl(string text)
        {
            Uri uri;
            return Uri.TryCreate(text, UriKind.Absolute, out uri) &&
                   (uri.Scheme.Equals("http", StringComparison.InvariantCultureIgnoreCase) ||
                   uri.Scheme.Equals("https", StringComparison.InvariantCultureIgnoreCase));
        }

        private ITextSnapshot GetSnapshot(ICodeDocument document)
        {
            var parseData = document.ParseData as LLParseData;
            if (parseData != null)
            {
                return parseData.Snapshot;
            }
            else
            {
                return document.CurrentSnapshot;
            }
        }
    }
}
