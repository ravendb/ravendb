//-----------------------------------------------------------------------
// <copyright file="DynamicQueryMappingItem.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Database.Data
{
    public class DynamicQueryMappingItem
    {
        public string From
        {
            get;
            set;
        }

        public string To
        {
            get;
            set;
        }
    }
}
