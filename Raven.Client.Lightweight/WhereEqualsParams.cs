namespace Raven.Client
{
	/// <summary>
	/// Parameters for the Where Equals call
	/// </summary>
	public class WhereEqualsParams
	{
		/// <summary>
		/// Create a new instance 
		/// </summary>
		public WhereEqualsParams()
		{
			IsNestedPath = false;
			AllowWildcards = false;
			IsAnalyzed = true;
		}

		/// <summary>
		/// The field name
		/// </summary>
		public string FieldName { get; set; }

		/// <summary>
		/// The field value
		/// </summary>
		public object Value { get; set; }

		/// <summary>
		/// Should the field be analyzed
		/// </summary>
		public bool IsAnalyzed { get; set; }


		/// <summary>
		/// Should the field allow wildcards
		/// </summary>
		public bool AllowWildcards { get; set; }

		/// <summary>
		/// Is this a root property or not?
		/// </summary>
		public bool IsNestedPath { get; set; }
	}
}