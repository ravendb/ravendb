//-----------------------------------------------------------------------
// <copyright file="WarningMessagesHolder.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;

namespace Raven.Database.Commercial
{
	public class WarningMessagesHolder
	{
		public HashSet<string> Messages
		{
			get; set;
		}

		public WarningMessagesHolder()
		{
			Messages = new HashSet<string>();
		}


		public static void AddWarning(DocumentDatabase db, string prefix, string msg)
		{
			var document = db.Get("Raven/WarningMessages", null);
			WarningMessagesHolder messagesHolder = document == null
			                                       	? new WarningMessagesHolder()
			                                       	: document.DataAsJson.JsonDeserialization<WarningMessagesHolder>();

			var message = prefix + ": " + msg;

			if (messagesHolder.Messages.Add(message) == false)
			{
				return; //already there
			}

			// remove anything else with this prefix
			messagesHolder.Messages.RemoveWhere(x => x.StartsWith(prefix) && x != message);

			db.Put("Raven/WarningMessages", null,
			       RavenJObject.FromObject(messagesHolder),
			       new RavenJObject(), null);
		}

		public static void RemoveWarnings(DocumentDatabase db, string prefix)
		{
			var document = db.Get("Raven/WarningMessages", null);
			WarningMessagesHolder messagesHolder = document == null
													? new WarningMessagesHolder()
													: document.DataAsJson.JsonDeserialization<WarningMessagesHolder>();

			// remove anything else with this prefix
			var removed = messagesHolder.Messages.RemoveWhere(x => x.StartsWith(prefix) );
			if (removed == 0)
				return;

			db.Put("Raven/WarningMessages", null,
				   RavenJObject.FromObject(messagesHolder),
				   new RavenJObject(), null);
		}
	}
}
