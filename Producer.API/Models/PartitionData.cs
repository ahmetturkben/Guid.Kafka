using Newtonsoft.Json;

namespace ProducerConsumer.API.Models
{
    public class PartitionData
    {
        [JsonProperty("partition")]
        public int Partition { get; set; }

        [JsonProperty("consumer_lag")]
        public int ConsumerLag { get; set; }
    }
}
