using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ProducerConsumer.API.Models
{
    public class KafkaConfiguration
    {
        public string Brokers { get; set; }
        public string Topic { get; set; }
        public string Key { get; set; }
        public string ConsumerGroup { get; set; }
    }
}
