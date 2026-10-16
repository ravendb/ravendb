using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace Raven.Migrator.MongoDB
{
    /// <summary>
    /// MongoDB.Driver 3.x defaults to GuidRepresentation.Unspecified and refuses to deserialize any UUID binary value.
    /// This serializer restores the 2.x driver default behavior: subtype 4 (UuidStandard) is read as Standard
    /// and subtype 3 (UuidLegacy) is read using the C# legacy byte order.
    /// </summary>
    internal sealed class MongoGuidSerializer : StructSerializerBase<Guid>
    {
        public override Guid Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        {
            var reader = context.Reader;
            var bsonType = reader.GetCurrentBsonType();
            switch (bsonType)
            {
                case BsonType.Binary:
                    var binaryData = reader.ReadBinaryData();
                    return binaryData.SubType switch
                    {
                        BsonBinarySubType.UuidStandard => GuidConverter.FromBytes(binaryData.Bytes, GuidRepresentation.Standard),
                        BsonBinarySubType.UuidLegacy => GuidConverter.FromBytes(binaryData.Bytes, GuidRepresentation.CSharpLegacy),
                        _ => throw new FormatException($"Cannot deserialize a Guid from binary subtype '{binaryData.SubType}'.")
                    };
                case BsonType.String:
                    return new Guid(reader.ReadString());
                default:
                    throw CreateCannotDeserializeFromBsonTypeException(bsonType);
            }
        }

        public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, Guid value)
        {
            context.Writer.WriteBinaryData(new BsonBinaryData(GuidConverter.ToBytes(value, GuidRepresentation.Standard), BsonBinarySubType.UuidStandard));
        }
    }
}
