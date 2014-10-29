// -----------------------------------------------------------------------
//  <copyright file="EscapingHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Text;

namespace Raven.Abstractions.Util
{
	public static class EscapingHelper
	{
		 public static string EscapeLongDataString(string data)
		 {
			 const int limit = 65519; // limit in .net 4.5 for EscapeDataString

			 if (data.Length <= limit)
			 {
				 return Uri.EscapeDataString(data);
			 }

			 var result = new StringBuilder();

			 int loops = data.Length / limit;

			 for (int i = 0; i <= loops; i++)
			 {
				 if (i < loops)
				 {
					 result.Append(Uri.EscapeDataString(data.Substring(limit * i, limit)));
				 }
				 else
				 {
					 result.Append(Uri.EscapeDataString(data.Substring(limit * i)));
				 }
			 }

			 return result.ToString();
		 }

		 public static string UnescapeLongDataString(string data)
		 {
			 const int limit = 65519; // limit in .net 4.5 for EscapeDataString

			 if (data.Length <= limit)
			 {
				 return Uri.UnescapeDataString(data);
			 }

			 var result = new StringBuilder();

			 int loops = data.Length / limit;

			 for (int i = 0; i <= loops; i++)
			 {
				 if (i < loops)
				 {
					 result.Append(Uri.UnescapeDataString(data.Substring(limit * i, limit)));
				 }
				 else
				 {
					 result.Append(Uri.UnescapeDataString(data.Substring(limit * i)));
				 }
			 }

			 return result.ToString();
		 }
	}
}