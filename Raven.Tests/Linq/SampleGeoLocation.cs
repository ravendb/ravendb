//-----------------------------------------------------------------------
// <copyright file="SampleGeoLocation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Tests.Linq
{
	public static class SampleGeoLocation
	{
		public static string GeoHash(int lon, int lang)
		{
			return lon + "#" + lang;
		}
	}
}