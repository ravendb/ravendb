// -----------------------------------------------------------------------
//  <copyright file="Constants.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Performance.Comparison
{
	public class Constants
	{
		public const int ItemsPerTransaction = 100;
		public const int WriteTransactions = 100 * 1000;
		public const int ReadItems = ItemsPerTransaction * WriteTransactions;
	}
}