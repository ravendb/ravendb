using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public class CoraxHighlightingTermIndex
    {
        public static string[] DefaultPreTags = {
            "<b style=\"background:yellow\">", "<b style=\"background:lawngreen\">", "<b style=\"background:aquamarine\">",
            "<b style=\"background:magenta\">", "<b style=\"background:palegreen\">", "<b style=\"background:coral\">",
            "<b style=\"background:wheat\">", "<b style=\"background:khaki\">", "<b style=\"background:lime\">",
            "<b style=\"background:deepskyblue\">", "<b style=\"background:deeppink\">", "<b style=\"background:salmon\">",
            "<b style=\"background:peachpuff\">", "<b style=\"background:violet\">", "<b style=\"background:mediumpurple\">",
            "<b style=\"background:palegoldenrod\">", "<b style=\"background:darkkhaki\">", "<b style=\"background:springgreen\">",
            "<b style=\"background:turquoise\">", "<b style=\"background:powderblue\">"
        };

        public static string[] DefaultPostTags = { "</b>" };


        public string GetPreTagByIndex(int i)
        {
            var preTags = PreTags != null ? PreTags : DefaultPreTags;
            if (preTags.Length == 1)
                return preTags[0];

            return i < preTags.Length ? preTags[i] : preTags[i % preTags.Length];            
        }

        public string GetPostTagByIndex(int i)
        {
            var postTags = PostTags != null ? PostTags : DefaultPostTags;
            if (postTags.Length == 1)
                return postTags[0];

            return i < postTags.Length ? postTags[i] : postTags[i % postTags.Length];
        }

        public string FieldName;
        public string DynamicFieldName;
        public object Values;
        public string[] PreTags;
        public string[] PostTags;
    }
}
