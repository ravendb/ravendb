using System;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Lexing;
using ActiproSoftware.Windows.Controls.SyntaxEditor.Outlining;
using ActiproSoftware.Windows.Controls.SyntaxEditor.Outlining.Implementation;

namespace ActiproSoftware.Windows.ProductSamples.SyntaxEditorSamples.QuickStart.CodeOutliningCollapsedText {

	/// <summary>
	/// Implements a multi-line comment <see cref="IOutliningNodeDefinition"/> that renders some of 
	/// a collapsed node's inner text.
	/// </summary>
	public class MultiLineCommentNodeDefinition : OutliningNodeDefinition {
		
		/////////////////////////////////////////////////////////////////////////////////////////////////////
		// OBJECT
		/////////////////////////////////////////////////////////////////////////////////////////////////////
		
		/// <summary>
		/// Initializes a new instance of the <c>MultiLineCommentNodeDefinition</c> class.
		/// </summary>
		public MultiLineCommentNodeDefinition() : base("MultiLineComment") {
			this.DefaultCollapsedContent = "/**/";
			this.IsDefaultCollapsed = true;
			this.IsImplementation = true;
		}
		
		/////////////////////////////////////////////////////////////////////////////////////////////////////
		// PUBLIC PROCEDURES
		/////////////////////////////////////////////////////////////////////////////////////////////////////

		/// <summary>
		/// Returns the content that should be displayed when the outlining node is collapsed.
		/// </summary>
		/// <param name="node">The <see cref="IOutliningNode"/>, based on this definition, for which content is requested.</param>
		/// <returns>The content that should be displayed when the outlining node is collapsed.</returns>
		/// <remarks>
		/// Only string-based content is currently supported.
		/// The default implementation of this method returns the value of the <see cref="DefaultCollapsedContent"/> property.
		/// This method can be overridden to generate unique collapsed content for a particular node.
		/// </remarks>
		public override object GetCollapsedContent(IOutliningNode node) {
			// Get the node's snapshot range
			TextSnapshotRange snapshotRange = node.SnapshotRange;

			// If the comment is over multiple lines...
			if (snapshotRange.StartPosition.Line < snapshotRange.EndPosition.Line) {
				// Use the text in the first line
				int lineEndOffset = snapshotRange.StartLine.EndOffset;
				return snapshotRange.Snapshot.GetSubstring(new TextRange(snapshotRange.StartOffset, lineEndOffset)) + "...";
			}
			else {
				// On a single line... use default collapsed content
				return this.DefaultCollapsedContent;
			}
		}
	
	}

}
