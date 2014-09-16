//-----------------------------------------------------------------------
// <copyright file="DatabaseRestoreRequest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Abstractions.Data
{
    public class DatabaseRestoreRequest : AbstractRestoreRequest
	{
		public string DatabaseName { get; set; }
	}
}