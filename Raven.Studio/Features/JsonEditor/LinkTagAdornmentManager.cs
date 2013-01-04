using System;
using System.Windows;
using System.Windows.Media;
using System.Windows.Shapes;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Tagging;
using ActiproSoftware.Text.Utility;
using ActiproSoftware.Windows.Controls.SyntaxEditor;
using ActiproSoftware.Windows.Controls.SyntaxEditor.Adornments;
using ActiproSoftware.Windows.Controls.SyntaxEditor.Adornments.Implementation;

namespace Raven.Studio.Features.JsonEditor
{
    public class LinkTagAdornmentManager : DecorationAdornmentManagerBase<IEditorView, LinkTag>
    {
        private static readonly AdornmentLayerDefinition layerDefinition =
            new AdornmentLayerDefinition("LinkTags", new Ordering(AdornmentLayerDefinitions.TextForeground.Key, OrderPlacement.After));

        public LinkTagAdornmentManager(IEditorView view) : base(view, layerDefinition)
        {
        }

        protected override void AddAdornment(ITextViewLine viewLine, TagSnapshotRange<LinkTag> tagRange, TextBounds bounds)
        {
            // Create the adornment
            var element = CreateDecorator(bounds.Width);
            var location = new Point(Math.Round(bounds.Left), bounds.Bottom - 2);

            // Add the adornment to the layer
            AdornmentLayer.AddAdornment(element, location, viewLine, tagRange.SnapshotRange, TextRangeTrackingModes.ExpandBothEdges, null);
        }

        private static UIElement CreateDecorator(double width)
        {
            // Create a rectangle
            var rect = new Rectangle
            {
                Width = width, Height = 1, Fill = new SolidColorBrush(Color.FromArgb(198, 144, 35, 35))
            };

            return rect;
        }
    }
}