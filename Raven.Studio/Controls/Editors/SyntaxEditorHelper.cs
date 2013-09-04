namespace Raven.Studio.Controls.Editors
{
	using System.IO;
	using System.Reflection;
	using ActiproSoftware.Text;
	using ActiproSoftware.Text.Implementation;

	///<summary>	
	/// Provides some helper methods.	
	///</summary>	
	public static class SyntaxEditorHelper
	{
		public const string DefinitionPath = "Raven.Studio.Controls.Editors.Definitions.";

		/// <summary>
		/// Initializes an existing <see cref="ISyntaxLanguage"/> from a language definition (.langdef file) from a resource stream.
		/// </summary>		
		/// <param name="language"></param>
		/// <param name="filename">The filename.</param>
		public static void InitializeLanguageFromResourceStream(ISyntaxLanguage language, string filename)
		{
			string path = DefinitionPath + filename;
			using (Stream stream = Assembly.GetExecutingAssembly().GetManifestResourceStream(path))
			{
				if (stream != null)
				{
					var serializer = new SyntaxLanguageDefinitionSerializer();
					serializer.InitializeFromStream(language, stream);
				}
			}
		}

		/// <summary>
		/// Loads a language definition (.langdef file) from a resource stream.
		/// </summary>
		/// <param name="filename">The filename.</param>
		/// <returns>The <see cref="ISyntaxLanguage"/> that was loaded.</returns>		
		public static ISyntaxLanguage LoadLanguageDefinitionFromResourceStream(string filename)
		{
			var path = DefinitionPath + filename;
			using (var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream(path))
			{
				if (stream != null)
				{
					var serializer = new SyntaxLanguageDefinitionSerializer();
					return serializer.LoadFromStream(stream);
				}
				
				return SyntaxLanguage.PlainText;
			}
		}
	}
}
