using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Consumer.API.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class ConsumerController : Controller
    {
        private readonly ConsumerConfi _cluster;

        [HttpPost]
        public IActionResult Post()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "host1:9092,host2:9092",
                GroupId = "foo",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            cluster.ConsumeFromLatest("demo");
            _cluster.MessageReceived += record =>
            {
                _logger.LogInformation($"Received: {Encoding.UTF8.GetString(record.Value as byte[])}");
            };

            return Task.CompletedTask;
            return Ok();
        }
    }
}
