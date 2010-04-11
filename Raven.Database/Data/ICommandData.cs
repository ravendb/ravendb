using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Database.Data
{
    public interface ICommandData
    {
		string Key { get; }
		string Method { get; }
    	void Execute(DocumentDatabase database);
    }
}
