using System;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Lexing;
using ActiproSoftware.Windows.Controls.SyntaxEditor.Outlining;
using ActiproSoftware.Windows.Controls.SyntaxEditor.Outlining.Implementation;

namespace ActiproSoftware.Windows.ProductSamples.SyntaxEditorSamples.QuickStart.CodeOutliningCollapsedText {

	/// <summary>
	/// Represents a <c>Javascript</c> language token-based outlining source.
	/// </summary>
	public class JavascriptOutliningSource : TokenOutliningSourceBase {

		private static OutliningNodeDefinition curlyBraceDefinition;
		private static MultiLineCommentNodeDefinition multiLineCommentDefinition;
		
		/////////////////////////////////////////////////////////////////////////////////////////////////////
		// OBJECT
		/////////////////////////////////////////////////////////////////////////////////////////////////////
		
		/// <summary>
		/// Initializes the <c>JavascriptOutliningSource</c> class.
		/// </summary>
		static JavascriptOutliningSource() {
			// Create the outlining node definitions that will be used by this outlining source to
			//   tell the document's outlining manager how to create new outlining nodes...

			curlyBraceDefinition = new OutliningNodeDefinition("CurlyBrace");
			curlyBraceDefinition.IsImplementation = true;
			
			multiLineCommentDefinition = new MultiLineCommentNodeDefinition();
		}
		
		/// <summary>
		/// Initializes a new instance of the <c>JavascriptOutliningSource</c> class.
		/// </summary>
		/// <param name="snapshot">The <see cref="ITextSnapshot"/> to use for this outlining source.</param>
		public JavascriptOutliningSource(ITextSnapshot snapshot) : base(snapshot) {}
		
		/////////////////////////////////////////////////////////////////////////////////////////////////////
		// PUBLIC PROCEDURES
		/////////////////////////////////////////////////////////////////////////////////////////////////////

		/// <summary>
		/// Returns information about the action to take when incrementally updating automatic outlining for a particular token.
		/// </summary>
		/// <param name="token">The <see cref="IToken"/> to examine.</param>
		/// <param name="definition">
		/// If the node action indicated is a start or end, an <see cref="IOutliningNodeDefinition"/> describing the related
		/// node must be returned.
		/// </param>
		/// <returns>
		/// An <see cref="OutliningNodeAction"/> indicating the action to take for the token.
		/// </returns>
		protected override OutliningNodeAction GetNodeActionForToken(IToken token, out IOutliningNodeDefinition definition) {
			switch (token.Key) {
				case "MultiLineCommentStartDelimiter":
					definition = multiLineCommentDefinition;
					return OutliningNodeAction.Start;
				case "MultiLineCommentEndDelimiter":
					definition = multiLineCommentDefinition;
					return OutliningNodeAction.End;
				case "OpenCurlyBrace":
					definition = curlyBraceDefinition;
					return OutliningNodeAction.Start;
				case "CloseCurlyBrace":
					definition = curlyBraceDefinition;
					return OutliningNodeAction.End;
				default:
					definition = null;
					return OutliningNodeAction.None;
			}
		}

	}

}
