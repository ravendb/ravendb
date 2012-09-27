//-----------------------------------------------------------------------
// <copyright file="MapOnlyView.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.ComponentModel;
using System.Linq;
using Raven.Database.Linq;

namespace Raven.Tests.Indexes
{
	[CLSCompliant(false)]
	[DisplayName("Compiled/View")]
	public class MapOnlyView : AbstractViewGenerator
	{
		public MapOnlyView()
		{
			AddField("CustomerId");
			AddMapDefinition(source => from doc in source select doc);
		}
	}
}