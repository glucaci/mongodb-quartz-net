using System.Text.Json;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace Quartz.Spi.MongoDbJobStore.Serializers
{
    internal class JobDataMapSerializer : SerializerBase<JobDataMap>
    {
        public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, JobDataMap value)
        {
            if (value == null)
            {
                context.Writer.WriteNull();
                return;
            }

            context.Writer.WriteString(JsonSerializer.Serialize(value));
        }

        public override JobDataMap Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
        {
            if (context.Reader.CurrentBsonType == BsonType.Null)
            {
                context.Reader.ReadNull();
                return null;
            }

            return JsonSerializer.Deserialize<JobDataMap>(context.Reader.ReadString());
        }
    }
}