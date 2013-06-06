//-----------------------------------------------------------------------
// <copyright file="SortedField.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Abstractions.Data
{
	/// <summary>
	/// Represent a field sort options
	/// </summary>
	public class SortedField
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="SortedField"/> class.
		/// </summary>
		/// <param name="fieldWithPotentialPrefix">The field with potential prefix.</param>
		public SortedField(string fieldWithPotentialPrefix)
		{
			if(fieldWithPotentialPrefix.StartsWith("+"))
			{
				Field = fieldWithPotentialPrefix.Substring(1);
			}
			else if (fieldWithPotentialPrefix.StartsWith("-"))
			{
				Field = fieldWithPotentialPrefix.Substring(1);
				Descending = true;
			}
			else
			{
				Field = fieldWithPotentialPrefix;
			}
		}

		/// <summary>
		/// Gets or sets the field.
		/// </summary>
		/// <value>The field.</value>
		public string Field { get; set; }
		/// <summary>
		/// Gets or sets a value indicating whether this <see cref="SortedField"/> is descending.
		/// </summary>
		/// <value><c>true</c> if descending; otherwise, <c>false</c>.</value>
		public bool Descending { get; set; }

	}
}
