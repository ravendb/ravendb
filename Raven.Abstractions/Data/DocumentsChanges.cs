using System;
namespace Raven.Abstractions.Data
{
    public class DocumentsChanges
    {
        public string FieldOldValue { get; set; }
        public string FieldNewValue { get; set; }
        public string FieldOldType { get; set; }
        public string FieldNewType { get; set; }
        public string Comment { get; set; }
        public string FieldName { get; set; }

        public enum CommentType
        {
            FieldChanged, DoesntExistInOriginal, DoesntExistInNew,FieldAdded,FieldRemoved
         }
  
   
        public static String CommentAsText( CommentType comment)
        {
              switch(comment)
              {
                    case CommentType.FieldAdded: return "field added ";
                    case CommentType.FieldRemoved: return "field removed ";
                    case CommentType.DoesntExistInNew: return "doesn't exist in new one ";
                    case CommentType.DoesntExistInOriginal: return "doesn't exist in original one ";
                    case CommentType.FieldChanged: return "field changed ";
                  default:
                      return string.Empty;

              }
        }
    
		protected bool Equals(DocumentsChanges other)
		{
			return string.Equals(FieldOldValue, other.FieldOldValue)
			       && string.Equals(FieldNewValue, other.FieldNewValue)
			       && string.Equals(FieldOldType, other.FieldOldType)
                   && string.Equals(FieldName, other.FieldName)
                   && string.Equals(FieldNewType, other.FieldNewType)
                   && string.Equals(Comment, other.Comment);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				int hashCode = (FieldOldValue != null ? FieldOldValue.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ (FieldNewValue != null ? FieldNewValue.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ (FieldOldType != null ? FieldOldType.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (FieldNewType != null ? FieldNewType.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (FieldName != null ? FieldName.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Comment != null ? Comment.GetHashCode() : 0);
				return hashCode;
			}
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != GetType()) return false;
			return Equals((DocumentsChanges) obj);
		}
	}
    
}