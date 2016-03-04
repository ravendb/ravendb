// -----------------------------------------------------------------------
//  <copyright file="BindingsHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Raven.Database.Util
{
    public static class BindingsHelper
    {

        public static readonly Regex SimpleBinding = new Regex(@"^\w+$", RegexOptions.Compiled);
        public static readonly Regex WordBinding = new Regex(@"\w+", RegexOptions.Compiled);

        /// <summary>
        /// Performs columns binding analysis. Extracts simple and compound bindings
        /// 
        /// Example of simple bindings are:
        /// Order, Summary, CustomerId
        /// 
        /// Following are NOT simple bindings:
        /// Order[0], FirstName + ' ' + LastName, min(Age, Date2)
        /// 
        /// For input: longerText(FirstName, LastName), Age
        ///   it returns longerText, FirstName, LastName as compound bindings
        ///   and Age as simple bounding
        /// </summary>
        public static BindingGroups AnalyzeBindings(string[] bindings)
        {
            var simpleBinding = new List<string>();
            var compoundBindings = new HashSet<string>(); // we don't care about order here

            foreach (var binding in bindings)
            {
                if (SimpleBinding.IsMatch(binding))
                {
                    simpleBinding.Add(binding);
                }
                else
                {
                    var tokens = ExtractTokens(binding);
                    compoundBindings.UnionWith(tokens);
                }
            }
            
            // if simple binding exists in compond list we don't want to send it twice
            simpleBinding = simpleBinding.Except(compoundBindings).ToList();

            return new BindingGroups
            {
                SimpleBindings = simpleBinding,
                CompoundBindings = compoundBindings.OrderBy(x => x).ToList()
            };
        }

        public static HashSet<string> ExtractTokens(string binding)
        {
            var result = new HashSet<string>();
            var matches = WordBinding.Matches(binding);
            foreach (var match in matches)
            {
                result.Add(match.ToString());
            }
            return result;
        }
    }

    public class BindingGroups
    {
        public List<string> SimpleBindings { get; set; }
        public List<string> CompoundBindings { get; set; }
    }
}