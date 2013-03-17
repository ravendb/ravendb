using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Windows.Media;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Implementation;
using ActiproSoftware.Text.Tagging;
using ActiproSoftware.Text.Tagging.Implementation;
using ActiproSoftware.Text.Utility;
using ActiproSoftware.Windows.Controls.SyntaxEditor;
using ActiproSoftware.Windows.Controls.SyntaxEditor.Highlighting;
using ActiproSoftware.Windows.Controls.SyntaxEditor.Highlighting.Implementation;

namespace Raven.Studio.Controls {

	/// <summary>
	/// Provides a custom implementation of a view-based classification tagger that tags the word that that view's caret is in.
	/// </summary>
	public class WordHighlightTagger : TaggerBase<IClassificationTag> {

		private IEditorView view;

		private static IClassificationType wordHighlightClassificationType = new ClassificationType("WordHighlight", "Word Highlight");

		private static Action HighlightedStringChanged;

		private static string highlightedString;
		public static string HighlightedString
		{
			get { return highlightedString; }
			set
			{
				highlightedString = value;
				var highlightedStringChanged = HighlightedStringChanged;
				if (highlightedStringChanged != null)
					highlightedStringChanged();
			}
		}

		public static string SearchBeforeClose { get; set; }

		/////////////////////////////////////////////////////////////////////////////////////////////////////
		// OBJECT
		/////////////////////////////////////////////////////////////////////////////////////////////////////
		
		/// <summary>
		/// Initializes the <c>WordHighlightTagger</c> class.
		/// </summary>
		static WordHighlightTagger() 
		{
			var brush = new SolidColorBrush(Colors.Yellow);
			AmbientHighlightingStyleRegistry.Instance.Register(wordHighlightClassificationType, new HighlightingStyle(null, brush));
		}

		public WordHighlightTagger() : base("Custom", new Ordering[] { new Ordering(TaggerKeys.Token, OrderPlacement.Before) }, new CodeDocument())
		{
			HighlightedString = "";
			SearchBeforeClose = "";
		}

		/// <summary>
		/// Initializes a new instance of the <c>WordHighlightTagger</c> class.
		/// </summary>
		/// <param name="view">The view to which this manager is attached.</param>
		public WordHighlightTagger(IEditorView view)
			: base("Custom", new Ordering[] {new Ordering(TaggerKeys.Token, OrderPlacement.Before)}, view.SyntaxEditor.Document)
		{

			// Initialize
			this.view = view;
			this.view.SelectionChanged += OnViewSelectionChanged;

			HighlightedStringChanged += UpdateHighlights;

			// Update current word
			UpdateHighlights();
		}

		/////////////////////////////////////////////////////////////////////////////////////////////////////
		// PUBLIC PROCEDURES
		/////////////////////////////////////////////////////////////////////////////////////////////////////
		
		/// <summary>
		/// Occurs when the view's selection is changed.
		/// </summary>
		/// <param name="sender">The sender of the event.</param>
		/// <param name="e">The <see cref="EditorViewSelectionEventArgs"/> that contains data related to this event.</param>
		private void OnViewSelectionChanged(object sender, EditorViewSelectionEventArgs e) {
			if (view == null)
				return;
			
			// Update the current word
			UpdateHighlights();
		}

		/// <summary>
		/// Updates the current word.
		/// </summary>
		private void UpdateHighlights()
		{
			if ((view == null) || (view.Selection == null))
				return;

			OnTagsChanged(new TagsChangedEventArgs(new TextSnapshotRange(view.SyntaxEditor.Document.CurrentSnapshot,
																		 view.SyntaxEditor.Document.CurrentSnapshot.TextRange)));
		}

		/////////////////////////////////////////////////////////////////////////////////////////////////////
		// PUBLIC PROCEDURES
		/////////////////////////////////////////////////////////////////////////////////////////////////////
		
		/// <summary>
		/// Returns the tag ranges that intersect with the specified normalized snapshot ranges.
		/// </summary>
		/// <param name="snapshotRanges">The collection of normalized snapshot ranges.</param>
		/// <param name="parameter">An optional parameter that provides contextual information about the tag request.</param>
		/// <returns>The tag ranges that intersect with the specified normalized snapshot ranges.</returns>
		public override IEnumerable<TagSnapshotRange<IClassificationTag>> GetTags(NormalizedTextSnapshotRangeCollection snapshotRanges, object parameter) {
			// Get a regex of the current word
			var search = new Regex(Regex.Escape(HighlightedString), RegexOptions.Singleline | RegexOptions.IgnoreCase);
			
			// Loop through the requested snapshot ranges...
			foreach (TextSnapshotRange snapshotRange in snapshotRanges) {
				// If the snapshot range is not zero-length...
				if (!snapshotRange.IsZeroLength) {
					// Look for current word matches
					foreach (Match match in search.Matches(snapshotRange.Text)) {
						// Add a highlighted range
						yield return new TagSnapshotRange<IClassificationTag>(new TextSnapshotRange(snapshotRange.Snapshot,
						                                                                            TextRange.FromSpan(
							                                                                            snapshotRange.StartOffset +
							                                                                            match.Index, match.Length)),
						                                                      new ClassificationTag(wordHighlightClassificationType));
					}
				}
			}
		}
		
		/// <summary>
		/// Occurs when the manager is closed and detached from the view.
		/// </summary>
		/// <remarks>
		/// The default implementation of this method does nothing.
		/// Overrides should release any event handlers set up in the manager's constructor.
		/// </remarks>
		protected override void OnClosed() {
			// Detach from the view
			if (view != null) {
				view.SelectionChanged -= new EventHandler<EditorViewSelectionEventArgs>(OnViewSelectionChanged);
				view = null;
			}

			// Call the base method
			base.OnClosed();
		}
	}
}